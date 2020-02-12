package logging

const (
	// TraceLevel indicates a log message's level of criticality
	TraceLevel = iota
	// DebugLevel indicates a log message's level of criticality
	DebugLevel
	// InfoLevel indicates a log message's level of criticality
	InfoLevel
	// WarnLevel indicates a log message's level of criticality
	WarnLevel
	// ErrorLevel indicates a log message's level of criticality
	ErrorLevel
	// FatalLevel indicates a log message's level of criticality
	FatalLevel
)

// LogLevelToString translates a log level enum to a string representation
func LogLevelToString(level int) string {
	switch level {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	case FatalLevel:
		return "FATAL"
	default:
		return "TRACE"
	}
}
