{{- define "kafka-2.5dc.controllerName" -}}
kraftcontroller-{{ .Values.region.name }}
{{- end -}}

{{/*
TLS secret used by Schema Registry. Precedence: tls.schemaRegistry.secretRef,
then the per-service cert-manager cert (tls.certManager.serviceCerts), then
the shared tls.secretRef.
*/}}
{{- define "kafka-2.5dc.tlsSecret.schemaRegistry" -}}
{{- if .Values.tls.schemaRegistry.secretRef -}}
{{ .Values.tls.schemaRegistry.secretRef }}
{{- else if and .Values.tls.certManager.enabled .Values.tls.certManager.serviceCerts.enabled -}}
{{ .Values.tls.certManager.serviceCerts.schemaRegistry.secretName }}
{{- else -}}
{{ .Values.tls.secretRef }}
{{- end -}}
{{- end -}}

{{/* Same, for Control Center. */}}
{{- define "kafka-2.5dc.tlsSecret.controlCenter" -}}
{{- if .Values.tls.controlCenter.secretRef -}}
{{ .Values.tls.controlCenter.secretRef }}
{{- else if and .Values.tls.certManager.enabled .Values.tls.certManager.serviceCerts.enabled -}}
{{ .Values.tls.certManager.serviceCerts.controlCenter.secretName }}
{{- else -}}
{{ .Values.tls.secretRef }}
{{- end -}}
{{- end -}}
