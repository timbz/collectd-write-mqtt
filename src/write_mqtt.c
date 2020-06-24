#include "collectd.h"

#include "common.h"
#include "plugin.h"
#include "utils_complain.h"
#include "utils_format_json.h"

#include <mosquitto.h>

#define WRITE_MQTT_MIN_MESSAGE_SIZE 1024
#define WRITE_MQTT_MAX_MESSAGE_SIZE 1024 * 128
#define WRITE_MQTT_DEFAULT_PORT 8883
#define WRITE_MQTT_DEFAULT_TOPIC "collectd"
#define WRITE_MQTT_KEEPALIVE 60

/*
 * Private variables
 */
struct wm_callback_s {
  char *name;

  struct mosquitto *mosq;
  _Bool connected;

  char *host;
  int port;
  char *client_id;
  char *capath;
  char *clientkey;
  char *clientcert;
  _Bool insecure;
  int protocol_version;
  int qos;
  char *topic;

  _Bool store_rates;

  char *send_buffer;
  size_t send_buffer_size;
  size_t send_buffer_free;
  size_t send_buffer_fill;
  cdtime_t send_buffer_init_time;

  c_complain_t complaint_cantpublish;
  pthread_mutex_t send_lock;
};
typedef struct wm_callback_s wm_callback_t;

static void wm_reset_buffer(wm_callback_t *cb) /* {{{ */
{
  if ((cb == NULL) || (cb->send_buffer == NULL))
    return;

  memset(cb->send_buffer, 0, cb->send_buffer_size);
  cb->send_buffer_free = cb->send_buffer_size;
  cb->send_buffer_fill = 0;
  cb->send_buffer_init_time = cdtime();

  format_json_initialize(cb->send_buffer, &cb->send_buffer_fill,
                         &cb->send_buffer_free);
} /* }}} wm_reset_buffer */

/* must hold cb->send_lock when calling. */
static int wm_mqtt_reconnect(wm_callback_t *cb) {
  int status;

  if (cb->connected)
    return (0);

  status = mosquitto_reconnect(cb->mosq);
  if (status != MOSQ_ERR_SUCCESS) {
    char errbuf[1024];
    ERROR("wm_mqtt_reconnect: mosquitto_reconnect failed: %s",
          (status == MOSQ_ERR_ERRNO) ? sstrerror(errno, errbuf, sizeof(errbuf))
                                     : mosquitto_strerror(status));
    return (-1);
  }

  cb->connected = 1;

  c_release(LOG_INFO, &cb->complaint_cantpublish,
            "write_mqtt plugin: successfully reconnected to broker \"%s:%d\"",
            cb->host, cb->port);

  return (0);
} /* wm_mqtt_reconnect */

/* must hold cb->send_lock when calling */
static int wm_publish_nolock(wm_callback_t *cb, char const *data) /* {{{ */
{
  int status;

  status = wm_mqtt_reconnect(cb);
  if (status != 0) {
    ERROR("write_mqtt plugin: unable to reconnect to broker");
    return (status);
  }

  status = mosquitto_publish(cb->mosq, /* message_id */ NULL, cb->topic,
                             (int)strlen(data), data,
                             cb->qos, /* retain */ false);
  if (status != MOSQ_ERR_SUCCESS) {
    char errbuf[1024];
    c_complain(LOG_ERR, &cb->complaint_cantpublish,
               "write_mqtt plugin: mosquitto_publish failed with %d: %s", status,
               (status == MOSQ_ERR_ERRNO)
                   ? sstrerror(errno, errbuf, sizeof(errbuf))
                   : mosquitto_strerror(status));
    /* Mark our connection "down" regardless of the error as a safety
     * measure; we will try to reconnect the next time we have to publish a
     * message */
    cb->connected = 0;
    mosquitto_disconnect(cb->mosq);

    return (-1);
  }

  return (0);
} /* }}} wm_publish_nolock */

/* must hold cb->send_lock when calling. */
static int wm_callback_init(wm_callback_t *cb) /* {{{ */
{
  char const *client_id;
  int status;

  if (cb->mosq != NULL)
    return (0);

  if (cb->client_id)
    client_id = cb->client_id;
  else
    client_id = hostname_g;

  cb->mosq =
      mosquitto_new(client_id, /* clean session */ true, /* user data */ cb);
  if (cb->mosq == NULL) {
    ERROR("write_mqtt plugin: mosquitto_new failed");
    return (-1);
  }

  mosquitto_opts_set(cb->mosq, MOSQ_OPT_PROTOCOL_VERSION, &cb->protocol_version);

  if (cb->capath) {
    status = mosquitto_tls_set(cb->mosq, cb->capath, NULL,
                               cb->clientcert, cb->clientkey,
                               /* pw_callback */ NULL);
    if (status != MOSQ_ERR_SUCCESS) {
      ERROR("write_mqtt plugin: cannot mosquitto_tls_set: %s",
            mosquitto_strerror(status));
      mosquitto_destroy(cb->mosq);
      cb->mosq = NULL;
      return (-1);
    }

    status = mosquitto_tls_insecure_set(cb->mosq, cb->insecure);
    if (status != MOSQ_ERR_SUCCESS) {
      ERROR("write_mqtt plugin: cannot mosquitto_tls_insecure_set: %s",
            mosquitto_strerror(status));
      mosquitto_destroy(cb->mosq);
      cb->mosq = NULL;
      return (-1);
    }
  }

  status =
      mosquitto_connect(cb->mosq, cb->host, cb->port, WRITE_MQTT_KEEPALIVE);
  if (status != MOSQ_ERR_SUCCESS) {
    char errbuf[1024];
    ERROR("write_mqtt plugin: mosquitto_connect failed: %s",
          (status == MOSQ_ERR_ERRNO) ? sstrerror(errno, errbuf, sizeof(errbuf))
                                     : mosquitto_strerror(status));

    mosquitto_destroy(cb->mosq);
    cb->mosq = NULL;
    return (-1);
  }

  cb->connected = 1;

  status = mosquitto_loop_start(cb->mosq);
  if (status != MOSQ_ERR_SUCCESS) {
    char errbuf[1024];
    ERROR("write_mqtt plugin: mosquitto_loop_start failed: %s",
          (status == MOSQ_ERR_ERRNO) ? sstrerror(errno, errbuf, sizeof(errbuf))
                                     : mosquitto_strerror(status));
    mosquitto_disconnect(cb->mosq);
    cb->connected = 1;
    mosquitto_destroy(cb->mosq);
    cb->mosq = NULL;
    return (-1);
  }

  wm_reset_buffer(cb);

  return (0);
} /* }}} int wm_callback_init */

static int wm_flush_nolock(cdtime_t timeout, wm_callback_t *cb) /* {{{ */
{
  int status;

  DEBUG("write_mqtt plugin: wm_flush_nolock: timeout = %.3f; "
        "send_buffer_fill = %zu;",
        CDTIME_T_TO_DOUBLE(timeout), cb->send_buffer_fill);

  /* timeout == 0  => flush unconditionally */
  if (timeout > 0) {
    cdtime_t now;

    now = cdtime();
    if ((cb->send_buffer_init_time + timeout) > now)
      return (0);
  }

  if (cb->send_buffer_fill <= 2) {
    cb->send_buffer_init_time = cdtime();
    return (0);
  }

  status = format_json_finalize(cb->send_buffer, &cb->send_buffer_fill,
                                &cb->send_buffer_free);
  if (status != 0) {
    ERROR("write_mqtt: wm_flush_nolock: "
          "format_json_finalize failed.");
    wm_reset_buffer(cb);
    return (status);
  }

  status = wm_publish_nolock(cb, cb->send_buffer);
  wm_reset_buffer(cb);

  return (status);
} /* }}} wm_flush_nolock */

static int wm_flush(cdtime_t timeout, /* {{{ */
                    const char *identifier __attribute__((unused)),
                    user_data_t *user_data) {
  wm_callback_t *cb;
  int status;

  if (user_data == NULL)
    return (-EINVAL);

  cb = user_data->data;

  pthread_mutex_lock(&cb->send_lock);

  if (wm_callback_init(cb) != 0) {
    ERROR("write_mqtt plugin: wm_callback_init failed.");
    pthread_mutex_unlock(&cb->send_lock);
    return (-1);
  }

  status = wm_flush_nolock(timeout, cb);
  pthread_mutex_unlock(&cb->send_lock);

  return (status);
} /* }}} int wm_flush */

static void wm_callback_free(void *data) /* {{{ */
{
  wm_callback_t *cb;

  if (data == NULL)
    return;

  cb = data;

  if (cb->mosq != NULL) {
    if (cb->send_buffer != NULL)
      wm_flush_nolock(/* timeout = */ 0, cb);

    if (cb->connected)
      (void)mosquitto_disconnect(cb->mosq);
    cb->connected = 0;

    (void)mosquitto_loop_stop(cb->mosq, false);

    (void)mosquitto_destroy(cb->mosq);
  }

  sfree(cb->name);
  sfree(cb->host);
  sfree(cb->client_id);
  sfree(cb->capath);
  sfree(cb->clientkey);
  sfree(cb->clientcert);
  sfree(cb->topic);
  sfree(cb->send_buffer);

  sfree(cb);
} /* }}} void wm_callback_free */

static int wm_write_json(const data_set_t *ds, const value_list_t *vl, /* {{{ */
                         wm_callback_t *cb) {
  int status;

  pthread_mutex_lock(&cb->send_lock);
  if (wm_callback_init(cb) != 0) {
    ERROR("write_mqtt plugin: wm_callback_init failed.");
    pthread_mutex_unlock(&cb->send_lock);
    return (-1);
  }

  status =
      format_json_value_list(cb->send_buffer, &cb->send_buffer_fill,
                             &cb->send_buffer_free, ds, vl, cb->store_rates);
  if (status == -ENOMEM) {
    status = wm_flush_nolock(/* timeout = */ 0, cb);
    if (status != 0) {
      wm_reset_buffer(cb);
      pthread_mutex_unlock(&cb->send_lock);
      return (status);
    }

    status =
        format_json_value_list(cb->send_buffer, &cb->send_buffer_fill,
                               &cb->send_buffer_free, ds, vl, cb->store_rates);
  }
  if (status != 0) {
    pthread_mutex_unlock(&cb->send_lock);
    return (status);
  }

  DEBUG("write_mqtt plugin: <%s> buffer %zu/%zu (%g%%)", cb->location,
        cb->send_buffer_fill, cb->send_buffer_size,
        100.0 * ((double)cb->send_buffer_fill) /
            ((double)cb->send_buffer_size));

  /* Check if we have enough space for this command. */
  pthread_mutex_unlock(&cb->send_lock);

  return (0);
} /* }}} int wm_write_json */

static int wm_write(const data_set_t *ds, const value_list_t *vl, /* {{{ */
                    user_data_t *user_data) {
  wm_callback_t *cb;
  int status;

  if (user_data == NULL)
    return (-EINVAL);

  cb = user_data->data;

  status = wm_write_json(ds, vl, cb);
  return (status);
} /* }}} int wm_write */

static int wm_config_node(oconfig_item_t *ci) /* {{{ */
{
  wm_callback_t *cb;
  char callback_name[DATA_MAX_NAME_LEN];
  int status = 0;

  cb = calloc(1, sizeof(*cb));
  if (cb == NULL) {
    ERROR("write_mqtt plugin: calloc failed.");
    return (-1);
  }

  cb->port = WRITE_MQTT_DEFAULT_PORT;
  cb->protocol_version = MQTT_PROTOCOL_V311;
  cb->topic = strdup(WRITE_MQTT_DEFAULT_TOPIC);
  cb->send_buffer_size = WRITE_MQTT_MAX_MESSAGE_SIZE;

  status = cf_util_get_string(ci, &cb->name);
  if (status != 0) {
    wm_callback_free(cb);
    return (status);
  }

  status = pthread_mutex_init(&cb->send_lock, /* attr = */ NULL);
  if (status != 0) {
    wm_callback_free(cb);
    return (status);
  }

  C_COMPLAIN_INIT(&cb->complaint_cantpublish);

  for (int i = 0; i < ci->children_num; i++) {
    oconfig_item_t *child = ci->children + i;

    if (strcasecmp("Host", child->key) == 0)
      status = cf_util_get_string(child, &cb->host);
    else if (strcasecmp("Port", child->key) == 0) {
      int port = cf_util_get_port_number(child);
      if (port < 0) {
        ERROR("write_mqtt plugin: Invalid port number.");
        status = EINVAL;
      } else
        cb->port = port;
    } else if (strcasecmp("ClientId", child->key) == 0)
      status = cf_util_get_string(child, &cb->client_id);
    else if (strcasecmp("CAPath", child->key) == 0)
      status = cf_util_get_string(child, &cb->capath);
    else if (strcasecmp("ClientKey", child->key) == 0)
      status = cf_util_get_string(child, &cb->clientkey);
    else if (strcasecmp("ClientCert", child->key) == 0)
      status = cf_util_get_string(child, &cb->clientcert);
    else if (strcasecmp("Insecure", child->key) == 0)
      status = cf_util_get_boolean(child, &cb->insecure);
    else if (strcasecmp("QoS", child->key) == 0) {
      int qos = -1;
      status = cf_util_get_int(child, &qos);
      if ((status != 0) || (qos < 0) || (qos > 1)) {
        ERROR("write_mqtt plugin: Not a valid QoS setting.");
        status = EINVAL;
      } else
        cb->qos = qos;
    } else if (strcasecmp("Topic", child->key) == 0)
      status = cf_util_get_string(child, &cb->topic);
    else if (strcasecmp("StoreRates", child->key) == 0)
      status = cf_util_get_boolean(child, &cb->store_rates);
    else if (strcasecmp("BufferSize", child->key) == 0) {
      int buffer_size = 0;
      status = cf_util_get_int(child, &buffer_size);
      if ((status != 0) || (buffer_size < WRITE_MQTT_MIN_MESSAGE_SIZE) || (buffer_size > WRITE_MQTT_MAX_MESSAGE_SIZE)) {
        ERROR("write_mqtt plugin: Not a valid BufferSize setting.");
        status = EINVAL;
      } else
        cb->send_buffer_size = buffer_size;
    } else {
      ERROR("write_mqtt plugin: Invalid configuration "
            "option: %s.",
            child->key);
      status = EINVAL;
    }

    if (status != 0)
      break;
  }

  if (status != 0) {
    wm_callback_free(cb);
    return (status);
  }

  if (cb->host == NULL) {
    ERROR("write_mqtt plugin: no Host defined for instance '%s'", cb->name);
    wm_callback_free(cb);
    return (-1);
  }

  /* Allocate the buffer. */
  cb->send_buffer = malloc(cb->send_buffer_size);
  if (cb->send_buffer == NULL) {
    ERROR("write_mqtt plugin: malloc(%zu) failed.", cb->send_buffer_size);
    wm_callback_free(cb);
    return (-1);
  }
  /* Nulls the buffer and sets ..._free and ..._fill. */
  wm_reset_buffer(cb);

  ssnprintf(callback_name, sizeof(callback_name), "write_mqtt/%s", cb->name);
  DEBUG("write_mqtt: Registering write callback '%s' with Host '%s'",
        callback_name, cb->host);

  plugin_register_write(callback_name, wm_write, &(user_data_t){
                                                     .data = cb,
                                                     .free_func = wm_callback_free,
                                                 });
  plugin_register_flush(callback_name, wm_flush, &(user_data_t){
                                                     .data = cb,
                                                 });

  return (0);
} /* }}} int wm_config_node */

static int wm_config(oconfig_item_t *ci) /* {{{ */
{
  for (int i = 0; i < ci->children_num; i++) {
    oconfig_item_t *child = ci->children + i;

    if (strcasecmp("Node", child->key) == 0)
      wm_config_node(child);
    else {
      ERROR("write_mqtt plugin: Invalid configuration "
            "option: %s.",
            child->key);
    }
  }

  return (0);
} /* }}} int wm_config */

static int wm_init(void) /* {{{ */
{
  mosquitto_lib_init();
  return (0);
} /* }}} int wm_init */

static int wm_shutdown(void) /* {{{ */
{
  mosquitto_lib_cleanup();
  return (0);
} /* }}} int wm_shutdown */

void module_register(void) /* {{{ */
{
  plugin_register_complex_config("write_mqtt", wm_config);
  plugin_register_init("write_mqtt", wm_init);
  plugin_register_shutdown ("write_mqtt", wm_shutdown);
} /* }}} void module_register */
