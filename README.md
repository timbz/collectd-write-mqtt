# Collectd Write MQTT plugin

The Write MQTT plugin sends the values collected by collectd to a MQTT broker. The data is formatted as JSON.

The plugin has beed developed and tested with version 5.7.

## Configuration

### Options

* **Host**: Hostname or IP-address of the MQTT broker.
* **Port**: Port number on which the MQTT broker accepts connections. Defaults to `8883`.
* **ClientId** MQTT client ID to use. Defaults to the hostname used by collectd.
* **CAPath** Path to the PEM-encoded CA certificate file.
* **ClientCert** Path to the PEM-encoded certificate file to use as client certificate when connecting to the MQTT broker. Only valid if *CAPath* and *ClientKey* are also set.
* **ClientKey** Path to the unencrypted PEM-encoded key file corresponding to *ClientCert*. Only valid if *CAPath* and *ClientCert* are also set.
* **Insecure** Configure verification of the server hostname in the server certificate. Defaults to `false`.
* **QoS** Sets the Quality of Service. Defautls to `0`.
* **Topic** Configures the topic to publish to. Defaults to `collectd`.
* **StoreRates** If set to `true`, convert counter values to rates. If set to `false` (the default) counter values are stored as is, i. e. as an increasing integer number.
* **BufferSize** Sets the send buffer size in Bytes. By increasing this buffer, less MQTT messages will be published, but more metrics will be batched / metrics are cached for longer before being sent, introducing additional delay until they are available on the server side. Bytes must be at least `1024` and cannot exceed `131072`. Defaults to `131072`.

### Sample `collectd.conf`

```
LoadPlugin write_mqtt

<Plugin write_mqtt>
   <Node "sample">
     Host "mqtt.sample.com"
     CAPath "/path/to/root.ca.pem"
     ClientCert "/path/to/cert.pem"
     ClientKey "/path/to/private.key"
   </Node>
</Plugin>
```
## Building

Copy `src/write_mqtt.c` into the collectd source and add following lines to the respective files:

* `src/Makefile.am`
    ```
    if BUILD_PLUGIN_WRITE_MQTT
    pkglib_LTLIBRARIES += write_mqtt.la
    write_mqtt_la_SOURCES = src/write_mqtt.c
    write_mqtt_la_CPPFLAGS = $(AM_CPPFLAGS) $(BUILD_WITH_LIBMOSQUITTO_CPPFLAGS)
    write_mqtt_la_LDFLAGS = $(PLUGIN_LDFLAGS) $(BUILD_WITH_LIBMOSQUITTO_LDFLAGS)
    write_mqtt_la_LIBADD = $(BUILD_WITH_LIBMOSQUITTO_LIBS) \
                        libformat_json.la
    endif
    ````
* `configure.ac`
    ```
    AC_PLUGIN([write_mqtt],          [$with_libmosquitto],      [MQTT json output plugin])
    ```
    ```
    AC_MSG_RESULT([    write_mqtt  . . . . . $enable_write_mqtt])
    ```