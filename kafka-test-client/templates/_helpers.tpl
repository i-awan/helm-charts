{{/* ---------- names and labels ---------- */}}

{{- define "ktc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ktc.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "ktc.labels" -}}
app.kubernetes.io/name: {{ include "ktc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/* call with (dict "root" . "component" "producer") */}}
{{- define "ktc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ktc.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* ---------- schema ConfigMap name / key ---------- */}}

{{- define "ktc.schemaConfigMapName" -}}
{{- if .Values.schema.existingConfigMap -}}
{{- .Values.schema.existingConfigMap -}}
{{- else -}}
{{- printf "%s-schema" (include "ktc.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "ktc.schemaConfigMapKey" -}}
{{- if .Values.schema.existingConfigMap -}}
{{- .Values.schema.existingConfigMapKey -}}
{{- else -}}
schema
{{- end -}}
{{- end -}}

{{/* ---------- environment shared by producer, consumer and test ---------- */}}

{{- define "ktc.commonEnv" -}}
{{- if not (has .Values.schema.format (list "avro" "jsonschema")) }}
{{- fail "schema.format must be 'avro' or 'jsonschema'" }}
{{- end }}
{{- if not (has .Values.kafka.securityProtocol (list "PLAINTEXT" "SSL" "SASL_SSL" "SASL_PLAINTEXT")) }}
{{- fail "kafka.securityProtocol must be one of PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT" }}
{{- end -}}
- name: KAFKA_BOOTSTRAP_SERVERS
  value: {{ required "kafka.bootstrapServers is required" .Values.kafka.bootstrapServers | quote }}
- name: KAFKA_SECURITY_PROTOCOL
  value: {{ .Values.kafka.securityProtocol | quote }}
{{- if or (eq .Values.kafka.securityProtocol "SSL") (eq .Values.kafka.securityProtocol "SASL_SSL") }}
- name: KAFKA_SSL_ENDPOINT_ID
  value: {{ ternary "https" "none" .Values.kafka.tls.verifyHostname | quote }}
{{- if .Values.kafka.tls.existingSecret }}
- name: KAFKA_SSL_CA
  value: /etc/kafka-tls/ca.crt
{{- if .Values.kafka.tls.mutual }}
- name: KAFKA_SSL_CERT
  value: /etc/kafka-tls/tls.crt
- name: KAFKA_SSL_KEY
  value: /etc/kafka-tls/tls.key
{{- end }}
{{- end }}
{{- end }}
{{- if or (eq .Values.kafka.securityProtocol "SASL_SSL") (eq .Values.kafka.securityProtocol "SASL_PLAINTEXT") }}
{{- $secret := required "kafka.sasl.existingSecret is required with a SASL securityProtocol" .Values.kafka.sasl.existingSecret }}
- name: KAFKA_SASL_MECHANISM
  value: {{ required "kafka.sasl.mechanism is required with a SASL securityProtocol" .Values.kafka.sasl.mechanism | quote }}
{{- if eq .Values.kafka.sasl.mechanism "OAUTHBEARER" }}
- name: KAFKA_OAUTH_TOKEN_URL
  value: {{ required "kafka.sasl.oauth.tokenEndpointUrl is required for OAUTHBEARER" .Values.kafka.sasl.oauth.tokenEndpointUrl | quote }}
{{- if .Values.kafka.sasl.oauth.scope }}
- name: KAFKA_OAUTH_SCOPE
  value: {{ .Values.kafka.sasl.oauth.scope | quote }}
{{- end }}
- name: KAFKA_OAUTH_CLIENT_ID
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ .Values.kafka.sasl.oauth.clientIdKey }}
- name: KAFKA_OAUTH_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ .Values.kafka.sasl.oauth.clientSecretKey }}
{{- else }}
- name: KAFKA_SASL_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ .Values.kafka.sasl.usernameKey }}
- name: KAFKA_SASL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ .Values.kafka.sasl.passwordKey }}
{{- end }}
{{- end }}
{{- if .Values.kafka.extraProperties }}
- name: KAFKA_EXTRA_PROPS
  value: {{ .Values.kafka.extraProperties | toJson | quote }}
{{- end }}
{{- if .Values.schemaRegistry.enabled }}
- name: SR_ENABLED
  value: "true"
- name: SR_URL
  value: {{ required "schemaRegistry.url is required when schemaRegistry.enabled=true" .Values.schemaRegistry.url | quote }}
- name: SR_AUTO_REGISTER
  value: {{ .Values.schemaRegistry.autoRegister | quote }}
- name: SR_USE_LATEST
  value: {{ .Values.schemaRegistry.useLatestVersion | quote }}
{{- if .Values.schemaRegistry.subject }}
- name: SR_SUBJECT
  value: {{ .Values.schemaRegistry.subject | quote }}
{{- end }}
{{- if and (hasPrefix "https" .Values.schemaRegistry.url) .Values.kafka.tls.existingSecret }}
- name: SR_SSL_CA
  value: /etc/kafka-tls/ca.crt
{{- if .Values.schemaRegistry.mutualTls }}
- name: SR_SSL_CERT
  value: /etc/kafka-tls/tls.crt
- name: SR_SSL_KEY
  value: /etc/kafka-tls/tls.key
{{- end }}
{{- end }}
{{- if .Values.schemaRegistry.basicAuth.existingSecret }}
- name: SR_BASIC_AUTH
  valueFrom:
    secretKeyRef:
      name: {{ .Values.schemaRegistry.basicAuth.existingSecret }}
      key: {{ .Values.schemaRegistry.basicAuth.key }}
{{- end }}
{{- else }}
- name: SR_ENABLED
  value: "false"
{{- end }}
- name: SCHEMA_FORMAT
  value: {{ .Values.schema.format | quote }}
- name: SCHEMA_PATH
  value: /schema/schema
- name: TOPIC
  value: {{ required "topic.name is required" .Values.topic.name | quote }}
- name: TOPIC_CREATE
  value: {{ .Values.topic.create | quote }}
- name: TOPIC_PARTITIONS
  value: {{ .Values.topic.partitions | quote }}
- name: TOPIC_REPLICATION_FACTOR
  value: {{ .Values.topic.replicationFactor | quote }}
{{- if .Values.topic.config }}
- name: TOPIC_CONFIG
  value: {{ .Values.topic.config | toJson | quote }}
{{- end }}
- name: LOG_LEVEL
  value: {{ .Values.logLevel | quote }}
- name: LOG_SAMPLES
  value: {{ .Values.logSamples | quote }}
{{- with .Values.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* ---------- role-specific environment ---------- */}}

{{- define "ktc.producerEnv" -}}
- name: MESSAGE_COUNT
  value: {{ .Values.producer.messageCount | quote }}
- name: RATE_PER_SECOND
  value: {{ .Values.producer.ratePerSecond | quote }}
- name: KEY_MODE
  value: {{ .Values.producer.key | quote }}
- name: PRODUCER_ACKS
  value: {{ .Values.producer.acks | quote }}
{{- if ne (toString .Values.producer.seed) "" }}
- name: SEED
  value: {{ toString .Values.producer.seed | quote }}
{{- end }}
{{- if .Values.producer.extraProperties }}
- name: PRODUCER_EXTRA_PROPS
  value: {{ .Values.producer.extraProperties | toJson | quote }}
{{- end }}
{{- end -}}

{{- define "ktc.consumerEnv" -}}
- name: CONSUMER_GROUP
  value: {{ default (printf "%s-consumer" (include "ktc.fullname" .)) .Values.consumer.groupId | quote }}
- name: AUTO_OFFSET_RESET
  value: {{ .Values.consumer.autoOffsetReset | quote }}
- name: MAX_MESSAGES
  value: {{ .Values.consumer.maxMessages | quote }}
- name: IDLE_TIMEOUT_SECONDS
  value: {{ .Values.consumer.idleTimeoutSeconds | quote }}
- name: FAIL_ON_INVALID
  value: {{ .Values.consumer.failOnInvalid | quote }}
{{- if .Values.consumer.extraProperties }}
- name: CONSUMER_EXTRA_PROPS
  value: {{ .Values.consumer.extraProperties | toJson | quote }}
{{- end }}
{{- end -}}

{{- define "ktc.testEnv" -}}
- name: MESSAGE_COUNT
  value: {{ .Values.test.messageCount | quote }}
- name: ROUNDTRIP_TIMEOUT
  value: {{ .Values.test.timeoutSeconds | quote }}
{{- if .Values.producer.extraProperties }}
- name: PRODUCER_EXTRA_PROPS
  value: {{ .Values.producer.extraProperties | toJson | quote }}
{{- end }}
{{- if .Values.consumer.extraProperties }}
- name: CONSUMER_EXTRA_PROPS
  value: {{ .Values.consumer.extraProperties | toJson | quote }}
{{- end }}
{{- end -}}

{{/* ---------- volumes ---------- */}}

{{- define "ktc.volumeMounts" -}}
- name: app
  mountPath: /app
  readOnly: true
- name: schema
  mountPath: /schema
  readOnly: true
- name: tmp
  mountPath: /tmp
{{- if .Values.kafka.tls.existingSecret }}
- name: kafka-tls
  mountPath: /etc/kafka-tls
  readOnly: true
{{- end }}
{{- with .Values.extraVolumeMounts }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* defaultMode values are decimal: 365 = 0555, 292 = 0444 */}}
{{- define "ktc.volumes" -}}
- name: app
  configMap:
    name: {{ include "ktc.fullname" . }}-app
    defaultMode: 365
- name: schema
  configMap:
    name: {{ include "ktc.schemaConfigMapName" . }}
    items:
      - key: {{ include "ktc.schemaConfigMapKey" . }}
        path: schema
- name: tmp
  emptyDir: {}
{{- if .Values.kafka.tls.existingSecret }}
- name: kafka-tls
  secret:
    secretName: {{ .Values.kafka.tls.existingSecret }}
    defaultMode: 292
    items:
      - key: {{ .Values.kafka.tls.caKey }}
        path: ca.crt
{{- if or .Values.kafka.tls.mutual .Values.schemaRegistry.mutualTls }}
      - key: {{ .Values.kafka.tls.certKey }}
        path: tls.crt
      - key: {{ .Values.kafka.tls.keyKey }}
        path: tls.key
{{- end }}
{{- end }}
{{- with .Values.extraVolumes }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* ---------- pod spec shared by producer, consumer and the test pod ----------
     call with (dict "root" . "mode" "produce" "roleEnv" "<yaml>" "restartPolicy" "Never" "resources" <map>) */}}
{{- define "ktc.podSpec" -}}
{{- $root := .root -}}
{{- with $root.Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- if $root.Values.serviceAccountName }}
serviceAccountName: {{ $root.Values.serviceAccountName }}
{{- end }}
restartPolicy: {{ .restartPolicy }}
securityContext:
  {{- toYaml $root.Values.podSecurityContext | nindent 2 }}
containers:
  - name: client
    image: {{ printf "%s:%s" (required "image.repository is required - build image/Dockerfile and push it" $root.Values.image.repository) (toString $root.Values.image.tag) | quote }}
    imagePullPolicy: {{ $root.Values.image.pullPolicy }}
    command: ["python", "/app/client.py"]
    args: [{{ .mode | quote }}]
    securityContext:
      {{- toYaml $root.Values.containerSecurityContext | nindent 6 }}
    env:
      {{- include "ktc.commonEnv" $root | nindent 6 }}
      {{- if .roleEnv }}
      {{- .roleEnv | nindent 6 }}
      {{- end }}
    resources:
      {{- toYaml .resources | nindent 6 }}
    volumeMounts:
      {{- include "ktc.volumeMounts" $root | nindent 6 }}
volumes:
  {{- include "ktc.volumes" $root | nindent 2 }}
{{- with $root.Values.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $root.Values.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $root.Values.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}
