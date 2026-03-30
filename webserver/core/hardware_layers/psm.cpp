//-----------------------------------------------------------------------------
// Copyright 2015 Thiago Alves
//
// Based on the LDmicro software by Jonathan Westhues
// This file is part of the OpenPLC Software Stack.
//
// OpenPLC is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// OpenPLC is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with OpenPLC.  If not, see <http://www.gnu.org/licenses/>.
//------
//
// Hybrid PSM + GPIO hardware layer:
//  - Digital I/O (%IX/%QX) is handled directly through wiringPi for deterministic behavior.
//  - PSM socket I/O (analog register exchange and stop handshake) runs only on a background thread.
//-----------------------------------------------------------------------------

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <wiringPi.h>
#include <pthread.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>

#include <atomic>

#include "ladder.h"

#if !defined(ARRAY_SIZE)
    #define ARRAY_SIZE(x) (sizeof((x)) / sizeof((x)[0]))
#endif

#define MB_PORT                 2605
#define MB_UID                  0
#define ERROR_LIMIT             10
#define ANALOG_REG_COUNT        50
#define KILL_REG_ADDR           50
#define KILL_SIGNAL             127
#define SOCKET_TIMEOUT_MS       200
#define READ_RETRY_LIMIT        30
#define COMM_LOOP_DELAY_MS      20
#define RECONNECT_DELAY_MS      500

#define MAX_INPUT               14
#define MAX_OUTPUT              11
#define MAX_ANALOG_OUT          1

// Raspberry Pi deterministic mappings for %IX/%QX
int inBufferPinMask[MAX_INPUT] = { 8, 9, 7, 0, 2, 3, 12, 13, 14, 21, 22, 23, 24, 25 };
int outBufferPinMask[MAX_OUTPUT] = { 15, 16, 4, 5, 6, 10, 11, 26, 27, 28, 29 };
int analogOutBufferPinMask[MAX_ANALOG_OUT] = { 1 };

static std::atomic<int> error_count(0);
static std::atomic<bool> stop_requested(false);
static std::atomic<bool> comm_thread_started(false);
static std::atomic<bool> psm_runner_started(false);
static std::atomic<bool> connected(false);

static pthread_t comm_thread;
static pthread_t psm_thread;
static int psm_socket = -1;
static pthread_mutex_t socketLock = PTHREAD_MUTEX_INITIALIZER;

static uint16_t analog_inputs_shadow[ANALOG_REG_COUNT] = {0};
static uint16_t analog_outputs_shadow[ANALOG_REG_COUNT] = {0};

static void log_msg(const char *msg)
{
    openplc_log((char *)msg);
}

static void log_errno(const char *prefix)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "%s: %s\n", prefix, strerror(errno));
    log_msg(msg);
}

static bool check_error_limit_and_log()
{
    if (error_count.load() >= ERROR_LIMIT)
    {
        log_msg("PSM: Too many errors!\nPSM: PSM communication is disabled\n");
        return true;
    }
    return false;
}

//----------------------------------------------------------------------------- 
// PSM runner thread - starts python interpreter and streams logs
//-----------------------------------------------------------------------------
static void *start_psm(void *)
{
    log_msg("PSM: Starting PSM...\n");

    const char *commands[] = {
        "../.venv/bin/python3 -u ./core/psm/main.py 2>&1",
        "./.venv/bin/python3 -u ./core/psm/main.py 2>&1",
        "python3 -u ./core/psm/main.py 2>&1"
    };

    FILE *psm_proc = NULL;
    const char *used = NULL;
    for (size_t i = 0; i < ARRAY_SIZE(commands); i++)
    {
        psm_proc = popen(commands[i], "r");
        if (psm_proc != NULL)
        {
            used = commands[i];
            break;
        }
    }

    if (psm_proc == NULL)
    {
        log_msg("PSM: Failed to launch Python interpreter for PSM\n");
        return NULL;
    }

    char launch_msg[256];
    snprintf(launch_msg, sizeof(launch_msg), "PSM: Python launcher command: %s\n", used);
    log_msg(launch_msg);

    char buffer[512];
    while (!stop_requested.load() && fgets(buffer, sizeof(buffer), psm_proc) != NULL)
    {
        log_msg(buffer);
    }

    if (pclose(psm_proc) != 0)
    {
        log_msg("PSM: Python interpreter exited with non-zero status\n");
    }

    return NULL;
}

static void kill_psm()
{
    log_msg("PSM: Killing previous PSM modules...\n");
    FILE *psm_proc = popen("pgrep -f 'python3 -u ./core/psm/main.py'", "r");
    if (psm_proc == NULL)
    {
        log_msg("PSM: Failed to enumerate old PSM processes\n");
        return;
    }

    char line[128];
    while (fgets(line, sizeof(line), psm_proc) != NULL)
    {
        int pid = atoi(line);
        if (pid > 0)
        {
            char cmd[96];
            snprintf(cmd, sizeof(cmd), "kill -TERM %d >/dev/null 2>&1", pid);
            system(cmd);
        }
    }
    pclose(psm_proc);

    sleepms(200);
    psm_proc = popen("pgrep -f 'python3 -u ./core/psm/main.py'", "r");
    if (psm_proc == NULL) return;

    while (fgets(line, sizeof(line), psm_proc) != NULL)
    {
        int pid = atoi(line);
        if (pid > 0)
        {
            char cmd[96];
            snprintf(cmd, sizeof(cmd), "kill -KILL %d >/dev/null 2>&1", pid);
            system(cmd);
        }
    }
    pclose(psm_proc);
}

static int connect_to_psm(bool debug)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
    {
        if (debug) log_errno("PSM: Socket creation failed");
        return -1;
    }

    struct timeval timeout;
    timeout.tv_sec = SOCKET_TIMEOUT_MS / 1000;
    timeout.tv_usec = (SOCKET_TIMEOUT_MS % 1000) * 1000;

    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char *)&timeout, sizeof(timeout)) < 0)
    {
        if (debug) log_errno("PSM: setsockopt(SO_RCVTIMEO) failed");
        close(sock);
        return -1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(MB_PORT);

    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0)
    {
        if (debug) log_msg("PSM: Invalid 127.0.0.1 address\n");
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        if (debug) log_errno("PSM: Connection to PSM failed");
        close(sock);
        return -1;
    }

    return sock;
}

static bool read_exact(int fd, uint8_t *dst, size_t len)
{
    size_t got = 0;
    int retries = 0;

    while (got < len && !stop_requested.load())
    {
        ssize_t ret = recv(fd, dst + got, len - got, 0);
        if (ret > 0)
        {
            got += (size_t)ret;
            retries = 0;
            continue;
        }

        if (ret == 0)
        {
            return false;
        }

        if (errno == EINTR)
        {
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            retries++;
            if (retries >= READ_RETRY_LIMIT) return false;
            continue;
        }

        return false;
    }

    return (got == len);
}

static bool send_all(int fd, const uint8_t *data, size_t len)
{
    size_t sent = 0;
    while (sent < len && !stop_requested.load())
    {
        ssize_t ret = send(fd, data + sent, len - sent, 0);
        if (ret > 0)
        {
            sent += (size_t)ret;
            continue;
        }
        if (ret < 0 && errno == EINTR) continue;
        if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) continue;
        return false;
    }
    return sent == len;
}

static bool read_modbus_frame(int fd, uint8_t *frame, size_t max_len, size_t *frame_len)
{
    if (max_len < 7) return false;
    if (!read_exact(fd, frame, 7)) return false;

    uint16_t mbap_len = ((uint16_t)frame[4] << 8) | frame[5];
    if (mbap_len < 2 || (size_t)(6 + mbap_len) > max_len) return false;

    size_t remain = mbap_len - 1; // uid already read in byte 6
    if (!read_exact(fd, frame + 7, remain)) return false;

    *frame_len = 7 + remain;
    return true;
}

static bool read_analog_inputs_once(int fd)
{
    uint8_t request[12] = {0x00,0x01,0x00,0x00,0x00,0x06,MB_UID,0x04,0x00,0x00,0x00,ANALOG_REG_COUNT};
    uint8_t response[260];
    size_t response_len = 0;

    if (!send_all(fd, request, sizeof(request))) return false;
    if (!read_modbus_frame(fd, response, sizeof(response), &response_len)) return false;
    if (response_len != (size_t)(9 + ANALOG_REG_COUNT * 2)) return false;
    if (response[2] != 0x00 || response[3] != 0x00) return false;
    if (response[6] != MB_UID) return false;

    if (response[7] != 0x04) return false;
    if (response[8] != (ANALOG_REG_COUNT * 2)) return false;

    // MBAP length should be uid+fc+bytecount+data = 1+1+1+100 = 103
    uint16_t mbap_len = ((uint16_t)response[4] << 8) | response[5];
    if (mbap_len != (uint16_t)(3 + ANALOG_REG_COUNT * 2)) return false;

    uint16_t local_inputs[ANALOG_REG_COUNT];
    int k = 9;
    for (int i = 0; i < ANALOG_REG_COUNT; i++)
    {
        local_inputs[i] = (uint16_t)response[k + 1] | ((uint16_t)response[k] << 8);
        k += 2;
    }

    pthread_mutex_lock(&bufferLock);
    memcpy(analog_inputs_shadow, local_inputs, sizeof(local_inputs));
    pthread_mutex_unlock(&bufferLock);

    return true;
}

static bool write_analog_outputs_once(int fd)
{
    uint8_t request[113];
    memset(request, 0, sizeof(request));

    // MBAP
    request[0] = 0x00; request[1] = 0x02; // transaction id
    request[2] = 0x00; request[3] = 0x00; // protocol id
    request[4] = 0x00; request[5] = 0x6B; // length = 107 bytes follow (uid+pdu)
    request[6] = MB_UID;

    // PDU FC16
    request[7] = 0x10;
    request[8] = 0x00; request[9] = 0x00; // start address
    request[10] = 0x00; request[11] = ANALOG_REG_COUNT;
    request[12] = ANALOG_REG_COUNT * 2;

    uint16_t local_outputs[ANALOG_REG_COUNT];
    pthread_mutex_lock(&bufferLock);
    memcpy(local_outputs, analog_outputs_shadow, sizeof(local_outputs));
    pthread_mutex_unlock(&bufferLock);

    int k = 13;
    for (int i = 0; i < ANALOG_REG_COUNT; i++)
    {
        request[k++] = (uint8_t)(local_outputs[i] >> 8);
        request[k++] = (uint8_t)(local_outputs[i] & 0xFF);
    }

    uint8_t response[32];
    size_t response_len = 0;

    if (!send_all(fd, request, sizeof(request))) return false;
    if (!read_modbus_frame(fd, response, sizeof(response), &response_len)) return false;
    if (response_len < 12) return false;
    if (response[2] != 0x00 || response[3] != 0x00) return false;
    if (response[6] != MB_UID) return false;

    if (response[7] != 0x10) return false;
    if (response[10] != 0x00 || response[11] != ANALOG_REG_COUNT) return false;

    // MBAP length should be uid+fc+addr_hi+addr_lo+cnt_hi+cnt_lo = 6
    uint16_t mbap_len = ((uint16_t)response[4] << 8) | response[5];
    if (mbap_len != 6) return false;

    return true;
}

static bool send_stop_signal_once(int fd)
{
    uint8_t request[12] = {0x00,0x03,0x00,0x00,0x00,0x06,MB_UID,0x06,0x00,KILL_REG_ADDR,0x00,KILL_SIGNAL};
    uint8_t response[32];
    size_t response_len = 0;

    if (!send_all(fd, request, sizeof(request))) return false;
    if (!read_modbus_frame(fd, response, sizeof(response), &response_len)) return false;
    if (response_len < 12) return false;
    if (response[2] != 0x00 || response[3] != 0x00) return false;
    if (response[6] != MB_UID) return false;

    if (response[7] != 0x06) return false;
    if (response[8] != 0x00 || response[9] != KILL_REG_ADDR) return false;
    if (response[10] != 0x00 || response[11] != KILL_SIGNAL) return false;

    uint16_t mbap_len = ((uint16_t)response[4] << 8) | response[5];
    if (mbap_len != 6) return false;

    return true;
}

static void *psm_comm_loop(void *)
{
    int local_socket = -1;

    while (!stop_requested.load())
    {
        if (check_error_limit_and_log()) break;

        if (local_socket < 0)
        {
            connected.store(false);
            local_socket = connect_to_psm(false);
            if (local_socket < 0)
            {
                sleepms(RECONNECT_DELAY_MS);
                continue;
            }

            pthread_mutex_lock(&socketLock);
            psm_socket = local_socket;
            pthread_mutex_unlock(&socketLock);

            connected.store(true);
            error_count.store(0);
            log_msg("PSM: Connected to PSM\n");
        }

        bool ok = read_analog_inputs_once(local_socket) && write_analog_outputs_once(local_socket);
        if (!ok)
        {
            error_count.fetch_add(1);
            connected.store(false);
            log_msg("PSM: I/O exchange failed, reconnecting...\n");
            shutdown(local_socket, SHUT_RDWR);
            close(local_socket);
            local_socket = -1;
            pthread_mutex_lock(&socketLock);
            psm_socket = -1;
            pthread_mutex_unlock(&socketLock);
            sleepms(RECONNECT_DELAY_MS);
            continue;
        }

        sleepms(COMM_LOOP_DELAY_MS);
    }

    if (local_socket >= 0)
    {
        send_stop_signal_once(local_socket);
        shutdown(local_socket, SHUT_RDWR);
        close(local_socket);
    }

    pthread_mutex_lock(&socketLock);
    psm_socket = -1;
    pthread_mutex_unlock(&socketLock);
    connected.store(false);

    return NULL;
}

void initializeHardware()
{
    if (wiringPiSetup() == -1)
    {
        log_msg("PSM: wiringPiSetup failed\n");
    }

    for (int i = 0; i < MAX_INPUT; i++)
    {
        pinMode(inBufferPinMask[i], INPUT);
        if (i != 0 && i != 1) pullUpDnControl(inBufferPinMask[i], PUD_DOWN);
    }

    for (int i = 0; i < MAX_OUTPUT; i++)
    {
        pinMode(outBufferPinMask[i], OUTPUT);
    }

    for (int i = 0; i < MAX_ANALOG_OUT; i++)
    {
        pinMode(analogOutBufferPinMask[i], PWM_OUTPUT);
    }

    stop_requested.store(false);
    error_count.store(0);

    int probe = connect_to_psm(false);
    if (probe >= 0)
    {
        close(probe);
        kill_psm();
        sleepms(500);
    }

    if (pthread_create(&psm_thread, NULL, start_psm, NULL) == 0)
    {
        psm_runner_started.store(true);
    }
    else
    {
        log_msg("PSM: Failed to create PSM launch thread\n");
    }

    sleepms(2000);

    if (pthread_create(&comm_thread, NULL, psm_comm_loop, NULL) == 0)
    {
        comm_thread_started.store(true);
    }
    else
    {
        log_msg("PSM: Failed to create PSM communication thread\n");
    }
}

void finalizeHardware()
{
    stop_requested.store(true);

    if (comm_thread_started.load())
    {
        pthread_join(comm_thread, NULL);
        comm_thread_started.store(false);
    }

    if (psm_runner_started.load())
    {
        pthread_join(psm_thread, NULL);
        psm_runner_started.store(false);
    }
}

void updateBuffersIn()
{
    pthread_mutex_lock(&bufferLock);

    // deterministic local digital input reads
    for (int i = 0; i < MAX_INPUT; i++)
    {
        if (bool_input[i / 8][i % 8] != NULL)
        {
            *bool_input[i / 8][i % 8] = digitalRead(inBufferPinMask[i]);
        }
    }

    // comm thread owns socket I/O; scan thread only copies cached analog input values
    for (int i = 0; i < ANALOG_REG_COUNT; i++)
    {
        if (int_input[i] != NULL) *int_input[i] = analog_inputs_shadow[i];
    }

    pthread_mutex_unlock(&bufferLock);
}

void updateBuffersOut()
{
    pthread_mutex_lock(&bufferLock);

    // deterministic local digital output writes
    for (int i = 0; i < MAX_OUTPUT; i++)
    {
        if (bool_output[i / 8][i % 8] != NULL)
        {
            digitalWrite(outBufferPinMask[i], *bool_output[i / 8][i % 8]);
        }
    }

    // preserve local PWM behavior
    for (int i = 0; i < MAX_ANALOG_OUT; i++)
    {
        if (int_output[i] != NULL)
        {
            pwmWrite(analogOutBufferPinMask[i], (*int_output[i] / 64));
        }
    }

    // comm thread will flush this shadow to PSM
    for (int i = 0; i < ANALOG_REG_COUNT; i++)
    {
        if (int_output[i] != NULL) analog_outputs_shadow[i] = (uint16_t)(*int_output[i]);
    }

    pthread_mutex_unlock(&bufferLock);
}
