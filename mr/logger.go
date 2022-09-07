package mr

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

var debugStart time.Time
var debugVerbosity int

//"CLER": "#74b9ff"
//"DUPL": "#ffeaa7"
//"TRCK": "#a29bfe"
const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dMaster  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dReduce  logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dMap     logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
	dServ    logTopic = "SERV"
	dWorker  logTopic = "CLER"
	dDupl    logTopic = "DUPL"
	dTrck    logTopic = "TRCK"
)

type Logger struct {
	debugStart     time.Time
	debugVerbosity int
}

func NewLogger() *Logger {
	logger := &Logger{}
	logger.init()
	return logger
}

func (logger *Logger) init() {
	logger.debugVerbosity = logger.getVerbosity()
	logger.debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// Retrieve the verbosity level from an environment variable
func (logger *Logger) getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func (logger *Logger) Debug(topic logTopic, server int,
	format string, a ...interface{}) {

	if logger.debugVerbosity >= 1 {
		time := time.Since(logger.debugStart).Microseconds()
		time /= 1000
		var prefix string
		if server == -1 {
			prefix = fmt.Sprintf("%06d %v ", time, string(topic))
		} else {
			prefix = fmt.Sprintf("%06d %v [%d] ", time, string(topic), server)
		}

		format = prefix + format + "\n"
		fmt.Printf(format, a...)
	}
}

var logger = NewLogger()
