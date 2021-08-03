#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# The various SSL stores and certificates were created with the following commands:
# Requires use of JDK 8+ keytool command.
set -e
export PATH=$JAVA_HOME/bin:$PATH
KEY_PASS=securepass
STORE_PASS=securepass
START_DATE="2000/01/01 00:00:00"
CA_VALIDITY=365243
VALIDITY=36525


# Clean up existing files
# -----------------------
rm -f *.crt *.csr *.p12 *.jceks *.jks

# Create a key and self-signed certificate for the CA, to sign server certificate requests and use for trust:
# ----------------------------------------------------------------------------------------------------
keytool -storetype pkcs12 -keystore server-ca-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -alias server-ca -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Server Certification Authority, OU=Artemis, O=ActiveMQ" -startdate "$START_DATE" -validity $CA_VALIDITY -ext bc:c=ca:true
keytool -storetype pkcs12 -keystore server-ca-keystore.p12 -storepass $STORE_PASS -alias server-ca -exportcert -rfc > server-ca.crt

# Create trust store with the CA cert:
# -------------------------------------------------------
keytool -storetype pkcs12 -keystore server-ca-truststore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias server-ca -file server-ca.crt -noprompt
keytool -importkeystore -srckeystore server-ca-truststore.p12 -destkeystore server-ca-truststore.jceks -srcstoretype pkcs12 -deststoretype jceks -srcstorepass securepass -deststorepass securepass
keytool -importkeystore -srckeystore server-ca-truststore.p12 -destkeystore server-ca-truststore.jks -srcstoretype pkcs12 -deststoretype jks -srcstorepass securepass -deststorepass securepass

# Create a key pair for the server, and sign it with the CA:
# ----------------------------------------------------------
keytool -storetype pkcs12 -keystore server-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -alias server -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -startdate "$START_DATE" -validity $VALIDITY -ext bc=ca:false -ext eku=sA -ext san=dns:localhost,ip:127.0.0.1

keytool -storetype pkcs12 -keystore server-keystore.p12 -storepass $STORE_PASS -alias server -certreq -file server.csr
keytool -storetype pkcs12 -keystore server-ca-keystore.p12 -storepass $STORE_PASS -alias server-ca -gencert -rfc -infile server.csr -outfile server.crt -startdate "$START_DATE" -validity $VALIDITY -ext bc=ca:false -ext san=dns:localhost,ip:127.0.0.1

keytool -storetype pkcs12 -keystore server-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias server-ca -file server-ca.crt -noprompt
keytool -storetype pkcs12 -keystore server-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias server -file server.crt

keytool -importkeystore -srckeystore server-keystore.p12 -destkeystore server-keystore.jceks -srcstoretype pkcs12 -deststoretype jceks -srcstorepass securepass -deststorepass securepass
keytool -importkeystore -srckeystore server-keystore.p12 -destkeystore server-keystore.jks -srcstoretype pkcs12 -deststoretype jks -srcstorepass securepass -deststorepass securepass

# Create a key pair for the other server, and sign it with the CA:
# ----------------------------------------------------------
keytool -storetype pkcs12 -keystore other-server-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -alias other-server -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Other Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -startdate "$START_DATE" -validity $VALIDITY -ext bc=ca:false -ext san=dns:localhost,ip:127.0.0.1

keytool -storetype pkcs12 -keystore other-server-keystore.p12 -storepass $STORE_PASS -alias other-server -certreq -file other-server.csr
keytool -storetype pkcs12 -keystore server-ca-keystore.p12 -storepass $STORE_PASS -alias server-ca -gencert -rfc -infile other-server.csr -outfile other-server.crt -startdate "$START_DATE" -validity $VALIDITY -ext bc=ca:false -ext eku=sA -ext san=dns:localhost,ip:127.0.0.1

keytool -storetype pkcs12 -keystore other-server-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias server-ca -file server-ca.crt -noprompt
keytool -storetype pkcs12 -keystore other-server-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias other-server -file other-server.crt

keytool -importkeystore -srckeystore other-server-keystore.p12 -destkeystore other-server-keystore.jceks -srcstoretype pkcs12 -deststoretype jceks -srcstorepass securepass -deststorepass securepass
keytool -importkeystore -srckeystore other-server-keystore.p12 -destkeystore other-server-keystore.jks -srcstoretype pkcs12 -deststoretype jks -srcstorepass securepass -deststorepass securepass

# Create trust store with the other server cert:
# -------------------------------------------------------
keytool -storetype pkcs12 -keystore other-server-truststore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias other-server -file other-server.crt -noprompt
keytool -importkeystore -srckeystore other-server-truststore.p12 -destkeystore other-server-truststore.jceks -srcstoretype pkcs12 -deststoretype jceks -srcstorepass securepass -deststorepass securepass
keytool -importkeystore -srckeystore other-server-truststore.p12 -destkeystore other-server-truststore.jks -srcstoretype pkcs12 -deststoretype jks -srcstorepass securepass -deststorepass securepass

# Create a key pair for the broker with an unexpected hostname, and sign it with the CA:
# --------------------------------------------------------------------------------------
keytool -storetype pkcs12 -keystore unknown-server-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -alias unknown-server -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Unknown Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -startdate "$START_DATE" -validity $VALIDITY -ext bc=ca:false -ext eku=sA

keytool -storetype pkcs12 -keystore unknown-server-keystore.p12 -storepass $STORE_PASS -alias unknown-server -certreq -file unknown-server.csr
keytool -storetype pkcs12 -keystore server-ca-keystore.p12 -storepass $STORE_PASS -alias server-ca -gencert -rfc -infile unknown-server.csr -outfile unknown-server.crt -startdate "$START_DATE" -validity $VALIDITY -ext bc=ca:false -ext eku=sA

keytool -storetype pkcs12 -keystore unknown-server-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias server-ca -file server-ca.crt -noprompt
keytool -storetype pkcs12 -keystore unknown-server-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias unknown-server -file unknown-server.crt

keytool -importkeystore -srckeystore unknown-server-keystore.p12 -destkeystore unknown-server-keystore.jceks -srcstoretype pkcs12 -deststoretype jceks -srcstorepass securepass -deststorepass securepass
keytool -importkeystore -srckeystore unknown-server-keystore.p12 -destkeystore unknown-server-keystore.jks -srcstoretype pkcs12 -deststoretype jks -srcstorepass securepass -deststorepass securepass

# Create a key and self-signed certificate for the CA, to sign client certificate requests and use for trust:
# ----------------------------------------------------------------------------------------------------
keytool -storetype pkcs12 -keystore client-ca-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -alias client-ca -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Client Certification Authority, OU=Artemis, O=ActiveMQ" -startdate "$START_DATE" -validity $CA_VALIDITY -ext bc:c=ca:true
keytool -storetype pkcs12 -keystore client-ca-keystore.p12 -storepass $STORE_PASS -alias client-ca -exportcert -rfc > client-ca.crt

# Create trust store with the CA cert:
# -------------------------------------------------------
keytool -storetype pkcs12 -keystore client-ca-truststore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias client-ca -file client-ca.crt -noprompt
keytool -importkeystore -srckeystore client-ca-truststore.p12 -destkeystore client-ca-truststore.jceks -srcstoretype pkcs12 -deststoretype jceks -srcstorepass securepass -deststorepass securepass
keytool -importkeystore -srckeystore client-ca-truststore.p12 -destkeystore client-ca-truststore.jks -srcstoretype pkcs12 -deststoretype jks -srcstorepass securepass -deststorepass securepass

# Create a key pair for the client, and sign it with the CA:
# ----------------------------------------------------------
keytool -storetype pkcs12 -keystore client-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -alias client -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Client, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -startdate "$START_DATE" -validity $VALIDITY -ext bc=ca:false -ext eku=cA -ext san=dns:localhost,ip:127.0.0.1

keytool -storetype pkcs12 -keystore client-keystore.p12 -storepass $STORE_PASS -alias client -certreq -file client.csr
keytool -storetype pkcs12 -keystore client-ca-keystore.p12 -storepass $STORE_PASS -alias client-ca -gencert -rfc -infile client.csr -outfile client.crt -startdate "$START_DATE" -validity $VALIDITY -ext bc=ca:false -ext eku=cA -ext san=dns:localhost,ip:127.0.0.1

keytool -storetype pkcs12 -keystore client-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias client-ca -file client-ca.crt -noprompt
keytool -storetype pkcs12 -keystore client-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias client -file client.crt

keytool -importkeystore -srckeystore client-keystore.p12 -destkeystore client-keystore.jceks -srcstoretype pkcs12 -deststoretype jceks -srcstorepass securepass -deststorepass securepass
keytool -importkeystore -srckeystore client-keystore.p12 -destkeystore client-keystore.jks -srcstoretype pkcs12 -deststoretype jks -srcstorepass securepass -deststorepass securepass

# Create another key pair for the client, and sign it with the CA:
# ----------------------------------------------------------
keytool -storetype pkcs12 -keystore other-client-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -alias other-client -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Other Client, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -startdate "$START_DATE" -validity $VALIDITY -ext bc=ca:false -ext eku=cA -ext san=dns:localhost,ip:127.0.0.1

keytool -storetype pkcs12 -keystore other-client-keystore.p12 -storepass $STORE_PASS -alias other-client -certreq -file other-client.csr
keytool -storetype pkcs12 -keystore client-ca-keystore.p12 -storepass $STORE_PASS -alias client-ca -gencert -rfc -infile other-client.csr -outfile other-client.crt -startdate "$START_DATE" -validity $VALIDITY -ext bc=ca:false -ext eku=cA -ext san=dns:localhost,ip:127.0.0.1

keytool -storetype pkcs12 -keystore other-client-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias client-ca -file client-ca.crt -noprompt
keytool -storetype pkcs12 -keystore other-client-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias other-client -file other-client.crt

keytool -importkeystore -srckeystore other-client-keystore.p12 -destkeystore other-client-keystore.jceks -srcstoretype pkcs12 -deststoretype jceks -srcstorepass securepass -deststorepass securepass
keytool -importkeystore -srckeystore other-client-keystore.p12 -destkeystore other-client-keystore.jks -srcstoretype pkcs12 -deststoretype jks -srcstorepass securepass -deststorepass securepass

# Create a key pair for the client with an unexpected hostname, and sign it with the CA:
# ----------------------------------------------------------
keytool -storetype pkcs12 -keystore unknown-client-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -alias unknown-client -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Unknown Client, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -startdate "$START_DATE" -validity $VALIDITY -ext bc=ca:false -ext eku=cA

keytool -storetype pkcs12 -keystore unknown-client-keystore.p12 -storepass $STORE_PASS -alias unknown-client -certreq -file unknown-client.csr
keytool -storetype pkcs12 -keystore client-ca-keystore.p12 -storepass $STORE_PASS -alias client-ca -gencert -rfc -infile unknown-client.csr -outfile unknown-client.crt -startdate "$START_DATE" -validity $VALIDITY -ext bc=ca:false -ext eku=cA

keytool -storetype pkcs12 -keystore unknown-client-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias client-ca -file client-ca.crt -noprompt
keytool -storetype pkcs12 -keystore unknown-client-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias unknown-client -file unknown-client.crt

keytool -importkeystore -srckeystore unknown-client-keystore.p12 -destkeystore unknown-client-keystore.jceks -srcstoretype pkcs12 -deststoretype jceks -srcstorepass securepass -deststorepass securepass
keytool -importkeystore -srckeystore unknown-client-keystore.p12 -destkeystore unknown-client-keystore.jks -srcstoretype pkcs12 -deststoretype jks -srcstorepass securepass -deststorepass securepass

# Clean up working files
# -----------------------
rm -f *.crt *.csr
