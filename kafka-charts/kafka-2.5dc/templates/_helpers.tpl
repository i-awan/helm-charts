{{- define "kafka-2.5dc.controllerName" -}}
kraftcontroller-{{ .Values.region.name }}
{{- end -}}
