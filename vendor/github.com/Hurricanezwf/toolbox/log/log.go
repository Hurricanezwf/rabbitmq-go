package log

import (
	"fmt"
	"path/filepath"

	"github.com/astaxie/beego/logs"
)

var l *Log = NewLogger()

func Reset(lc *LogConf) {
	l.Apply(lc)
}

func Error(format string, v ...interface{}) {
	l.log.Error(format, v...)
}

func Warn(format string, v ...interface{}) {
	l.log.Warn(format, v...)
}

func Notice(format string, v ...interface{}) {
	l.log.Notice(format, v...)
}

func Info(format string, v ...interface{}) {
	l.log.Info(format, v...)
}

func Debug(format string, v ...interface{}) {
	l.log.Debug(format, v...)
}

////////////////////////////////////////////////////////////////////////////
type LogConf struct {
	logLevel int
	logWay   string
	logFile  string
}

func DefaultLogConf() *LogConf {
	return &LogConf{
		logLevel: logs.LevelInfo,
		logWay:   logs.AdapterConsole,
		logFile:  "",
	}
}

func (lc *LogConf) SetLogLevel(level string) {
	var lv int
	switch level {
	case "error":
		lv = logs.LevelError
	case "warn":
		lv = logs.LevelWarning
	case "notice":
		lv = logs.LevelNotice
	case "info":
		lv = logs.LevelInformational
	case "debug":
		lv = logs.LevelDebug
	default:
		lv = logs.LevelDebug
	}

	lc.logLevel = lv
}

func (lc *LogConf) LogLevel() string {
	switch lc.logLevel {
	case logs.LevelDebug:
		return "debug"
	case logs.LevelInformational:
		return "info"
	case logs.LevelNotice:
		return "notice"
	case logs.LevelWarning:
		return "warn"
	case logs.LevelError:
		return "error"
	}
	return ""
}

func (lc *LogConf) SetLogWay(way string) {
	if way == logs.AdapterConsole || way == logs.AdapterFile {
		lc.logWay = way
	}
}

func (lc *LogConf) SetLogFile(file string) {
	absPath, err := filepath.Abs(file)
	if err != nil {
		Error("Invalid filepath, %v", err)
		return
	}
	lc.logFile = absPath
}

func (lc *LogConf) Print() {
	Info("[LogConf]")
	Info("  %-24s : %s", "LogLevel", lc.LogLevel())
	Info("  %-24s : %s", "LogWay", lc.logWay)
	Info("  %-24s : %s", "LogFile", lc.logFile)
}

///////////////////////////////////////////////////////////////////////////////
type Log struct {
	log *logs.BeeLogger

	cfg *LogConf
}

func NewLogger() *Log {
	logger := logs.NewLogger()
	logger.EnableFuncCallDepth(true)
	logger.SetLogFuncCallDepth(3)

	return &Log{
		log: logger,
		cfg: DefaultLogConf(),
	}
}

// Apply applies all config to BeeLogger
func (l *Log) Apply(lc *LogConf) {
	if lc == nil {
		return
	}

	l.cfg = lc
	config := fmt.Sprintf("{\"filename\":\"%s\",\"level\":%d}", lc.logFile, lc.logLevel)
	l.log.DelLogger(lc.logWay)
	l.log.SetLogger(lc.logWay, config)
}
