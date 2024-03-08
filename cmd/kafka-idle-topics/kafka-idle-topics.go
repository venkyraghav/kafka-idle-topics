package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	VERSION                  = "1.2"
	PROGNAME                 = "kafka-idle-topics"
	GSSAPI                   = "GSSAPI"
	PLAIN                    = "PLAIN"
	GSSAPI_TLS               = "GSSAPI_TLS"
	PLAIN_TLS                = "PLAIN_TLS"
	TLS                      = "TLS"
	NONE                     = "NONE"
	KAFKA_BOOTSTRAP          = "KAFKA_BOOTSTRAP"
	KAFKA_USERNAME           = "KAFKA_USERNAME"
	KAFKA_PASSWORD           = "KAFKA_PASSWORD"
	KAFKA_GSSAPI_KEYTAB      = "KAFKA_GSSAPI_KEYTAB"
	KAFKA_GSSAPI_SERVICENAME = "KAFKA_GSSAPI_SERVICENAME"
	KAFKA_IDLE_MINUTES       = "KAFKA_IDLE_MINUTES"
	PRODUCTION               = "PRODUCTION"
	CONSUMPTION              = "CONSUMPTION"
	STORAGE                  = "STORAGE"
)

func NewKafkaIdleTopics() *KafkaIdleTopics {
	thisInstance := KafkaIdleTopics{}
	thisInstance.DeleteCandidates = make(map[string]bool)
	return &thisInstance
}

func ReadCommands() *KafkaIdleTopics {
	thisInstance := NewKafkaIdleTopics()

	flag.StringVar(&thisInstance.kafkaUrl, "bootstrap-servers", "", "Address to the target Kafka Cluster. Accepts multiple endpoints separated by a comma. Can be set using env variable KAFKA_BOOTSTRAP")
	flag.StringVar(&thisInstance.kafkaUsername, "username", "", "Username in the PLAIN module. Can be set using env variable KAFKA_USERNAME")
	flag.StringVar(&thisInstance.kafkaPassword, "password", "", "Password in the PLAIN module. Can be set using env variable KAFKA_PASSWORD")
	flag.StringVar(&thisInstance.kafkaSecurity, "kafkaSecurity", "none", "Type of connection to attempt. Options: plain_tls, plain (no tls), tls (one-way), none.")
	flag.StringVar(&thisInstance.fileName, "filename", "idleTopics.txt", "Custom filename for the output if needed.")
	flag.StringVar(&thisInstance.skip, "skip", "", "Filtering to skip. Options are: production, consumption, storage. This can be a comma-delimited list.")
	flag.IntVar(&thisInstance.productionAssessmentTime, "productionAssessmentTimeMs", 30000, "Timeframe to assess active production.")
	flag.Int64Var(&thisInstance.topicsIdleMinutes, "idleMinutes", 0, "Amount of minutes a topic should be idle to report it. Can be set using env variable KAFKA_IDLE_MINUTES")
	flag.BoolVar(&thisInstance.hideInternalTopics, "hideInternalTopics", false, "Hide internal topics from assessment.")
	flag.Var(&thisInstance.hideDerivativeTopics, "hideTopicsPrefixes", "Disqualify provided prefixes from assessment. A comma delimited list. It also accepts a path to a file containing a list.")
	flag.Var(&thisInstance.AllowList, "allowList", "A comma delimited list of topics to evaluate. It also accepts a path to a file containing a list of topics.")
	flag.Var(&thisInstance.DisallowList, "disallowList", "A comma delimited list of topics to exclude from evaluation. It also accepts a path to a file containing a list of topics.")
	flag.StringVar(&thisInstance.kafkaGssapiKeytab, "gssapiKeytab", "", "keytab filepath in the GSSAPI module. Can be set using env variable KAFKA_GSSAPI_KEYTAB")
	flag.StringVar(&thisInstance.kafkaGssapiServicename, "gssapiServicename", "", "Kafka service in the GSSAPI module. Can be set using env variable KAFKA_GSSAPI_SERVICENAME")
	versionFlag := flag.Bool("version", false, "Print the current version and exit")

	flag.Parse()

	if *versionFlag {
		fmt.Printf("%s: %s\n", PROGNAME, VERSION)
		os.Exit(0)
	}

	thisInstance.kafkaSecurity = strings.ToUpper(thisInstance.kafkaSecurity)
	if thisInstance.skip != "" {
		thisInstance.skip = strings.ToUpper(thisInstance.skip)
	}

	return thisInstance
}

func assertNotEmpty(property, value, message string) {
	if value == "" {
		log.Fatalf("%s %s", property, message)
	}
}

func getOsEnvOverride(property *string, envVar string) {
	if *property == "" {
		*property, _ = GetOSEnvVar(envVar)
	}
}
func main() {

	myChecker := ReadCommands()

	getOsEnvOverride(&myChecker.kafkaUrl, KAFKA_BOOTSTRAP)

	switch myChecker.kafkaSecurity {
	case PLAIN_TLS, PLAIN:
		getOsEnvOverride(&myChecker.kafkaUsername, KAFKA_USERNAME)
		getOsEnvOverride(&myChecker.kafkaPassword, KAFKA_PASSWORD)

		assertNotEmpty("Username", myChecker.kafkaUsername, "is required for PLAIN mechanism")
		assertNotEmpty("Password", myChecker.kafkaPassword, "is required for PLAIN mechanism")
	case GSSAPI_TLS, GSSAPI:
		getOsEnvOverride(&myChecker.kafkaGssapiKeytab, KAFKA_GSSAPI_KEYTAB)
		getOsEnvOverride(&myChecker.kafkaGssapiServicename, KAFKA_GSSAPI_SERVICENAME)

		assertNotEmpty("Keytab", myChecker.kafkaGssapiKeytab, "is required for GSSAPI mechanism")
		assertNotEmpty("Servicename", myChecker.kafkaGssapiServicename, "is required for GSSAPI mechanism")
	}

	if myChecker.topicsIdleMinutes == 0 {
		envVar, err := GetOSEnvVar(KAFKA_IDLE_MINUTES)
		if err != nil {
			myChecker.topicsIdleMinutes = 0
		} else {
			idleInt, err := strconv.ParseInt(envVar, 10, 64)
			if err != nil {
				log.Printf("Couldn't parse env var %v, using default of 0", err)
				myChecker.topicsIdleMinutes = 0
			}
			myChecker.topicsIdleMinutes = idleInt
		}
	}

	stepsToSkip := strings.Split(myChecker.skip, ",")

	// Extract Topics in Cluster
	myChecker.topicPartitionMap = myChecker.getClusterTopics(myChecker.getAdminClient(myChecker.kafkaSecurity))

	if !isInSlice(PRODUCTION, stepsToSkip) {
		if myChecker.topicsIdleMinutes == 0 {
			myChecker.filterActiveProductionTopics(myChecker.getClusterClient(myChecker.kafkaSecurity))
		} else {
			myChecker.filterTopicsIdleSince(myChecker.getClusterClient(myChecker.kafkaSecurity))
		}
	}

	if !isInSlice(CONSUMPTION, stepsToSkip) {
		myChecker.filterTopicsWithConsumerGroups(myChecker.getAdminClient(myChecker.kafkaSecurity))
	}

	if !isInSlice(STORAGE, stepsToSkip) {
		myChecker.filterEmptyTopics(myChecker.getClusterClient(myChecker.kafkaSecurity))
	}

	myChecker.filterOutDeleteCandidates()

	path := myChecker.writeDeleteCandidatesLocally()

	partitionCount := 0
	for _, ps := range myChecker.topicPartitionMap {
		partitionCount = partitionCount + len(ps)
	}

	log.Printf("Done! You can delete %v topics and %v partitions! A list of found idle topics is available at: %s", len(myChecker.topicPartitionMap), partitionCount, path)
}
