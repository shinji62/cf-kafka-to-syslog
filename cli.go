package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"

	"github.com/cloudfoundry-community/firehose-to-syslog/caching"
	"github.com/cloudfoundry-community/firehose-to-syslog/eventRouting"
	"github.com/cloudfoundry-community/firehose-to-syslog/logging"
	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/pkg/profile"
	kafkaconsumer "github.com/shinji62/cf-kafka-to-syslog/kafka-consumer"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	debug              = kingpin.Flag("debug", "Enable debug mode. This disables forwarding to syslog").Default("false").OverrideDefaultFromEnvar("DEBUG").Bool()
	apiEndpoint        = kingpin.Flag("api-endpoint", "Api endpoint address. For bosh-lite installation of CF: https://api.10.244.0.34.xip.io").OverrideDefaultFromEnvar("API_ENDPOINT").Required().String()
	syslogServer       = kingpin.Flag("syslog-server", "Syslog server.").OverrideDefaultFromEnvar("SYSLOG_ENDPOINT").String()
	syslogProtocol     = kingpin.Flag("syslog-protocol", "Syslog protocol (tcp/udp/tcp+tls).").Default("tcp").OverrideDefaultFromEnvar("SYSLOG_PROTOCOL").String()
	clientID           = kingpin.Flag("client-id", "Client ID.").OverrideDefaultFromEnvar("FIREHOSE_CLIENT_ID").Required().String()
	clientSecret       = kingpin.Flag("client-secret", "Client secret.").OverrideDefaultFromEnvar("FIREHOSE_CLIENT_SECRET").Required().String()
	skipSSLValidation  = kingpin.Flag("skip-ssl-validation", "Please don't").Default("false").OverrideDefaultFromEnvar("SKIP_SSL_VALIDATION").Bool()
	logEventTotals     = kingpin.Flag("log-event-totals", "Logs the counters for all selected events since nozzle was last started.").Default("false").OverrideDefaultFromEnvar("LOG_EVENT_TOTALS").Bool()
	logEventTotalsTime = kingpin.Flag("log-event-totals-time", "How frequently the event totals are calculated (in sec).").Default("30s").OverrideDefaultFromEnvar("LOG_EVENT_TOTALS_TIME").Duration()
	wantedEvents       = kingpin.Flag("events", fmt.Sprintf("Comma separated list of events you would like. Valid options are %s", eventRouting.GetListAuthorizedEventEvents())).Default("LogMessage").OverrideDefaultFromEnvar("EVENTS").String()
	boltDatabasePath   = kingpin.Flag("boltdb-path", "Bolt Database path ").Default("my.db").OverrideDefaultFromEnvar("BOLTDB_PATH").String()
	tickerTime         = kingpin.Flag("cc-pull-time", "CloudController Polling time in sec").Default("60s").OverrideDefaultFromEnvar("CF_PULL_TIME").Duration()
	extraFields        = kingpin.Flag("extra-fields", "Extra fields you want to annotate your events with, example: '--extra-fields=env:dev,something:other ").Default("").OverrideDefaultFromEnvar("EXTRA_FIELDS").String()
	modeProf           = kingpin.Flag("mode-prof", "Enable profiling mode, one of [cpu, mem, block]").Default("").OverrideDefaultFromEnvar("MODE_PROF").String()
	pathProf           = kingpin.Flag("path-prof", "Set the Path to write profiling file").Default("").OverrideDefaultFromEnvar("PATH_PROF").String()
	logFormatterType   = kingpin.Flag("log-formatter-type", "Log formatter type to use. Valid options are text, json. If none provided, defaults to json.").Envar("LOG_FORMATTER_TYPE").String()
	certPath           = kingpin.Flag("cert-pem-syslog", "Certificate Pem file").Envar("CERT_PEM").Default("").String()
	kfBrokersList      = kingpin.Flag("kafka-brokers", "Kafka broker list comma separated").Required().String()
	ignoreMissingApps  = kingpin.Flag("ignore-missing-apps", "Enable throttling on cache lookup for missing apps").Envar("IGNORE_MISSING_APPS").Default("false").Bool()
)

var (
	version = "0.0.0"
)

// CLI is the command line object
type CLI struct {
	// outStream and errStream are the stdout and stderr
	// to write message from the CLI.
	outStream, errStream io.Writer
}

// Run invokes the CLI with the given arguments.
func (cli *CLI) Run() int {
	kingpin.Parse()

	kingpin.Version(version)
	kingpin.Parse()

	//Setup Logging
	loggingClient := logging.NewLogging(*syslogServer, *syslogProtocol, *logFormatterType, *certPath, *debug)
	logging.LogStd(fmt.Sprintf("Starting firehose-to-syslog %s ", version), true)

	if *modeProf != "" {
		switch *modeProf {
		case "cpu":
			defer profile.Start(profile.CPUProfile, profile.ProfilePath(*pathProf)).Stop()
		case "mem":
			defer profile.Start(profile.MemProfile, profile.ProfilePath(*pathProf)).Stop()
		case "block":
			defer profile.Start(profile.BlockProfile, profile.ProfilePath(*pathProf)).Stop()
		default:
			// do nothing
		}
	}

	c := cfclient.Config{
		ApiAddress:        *apiEndpoint,
		ClientID:          *clientID,
		ClientSecret:      *clientSecret,
		SkipSslValidation: *skipSSLValidation,
		UserAgent:         "cf-kafka-syslog/" + version,
	}
	cfClient, err := cfclient.NewClient(&c)
	if err != nil {
		log.Fatal("New Client: ", err)
		os.Exit(1)

	}

	//Creating Caching
	var cachingClient caching.Caching
	if caching.IsNeeded(*wantedEvents) {
		config := &caching.CachingBoltConfig{
			Path:               *boltDatabasePath,
			IgnoreMissingApps:  *ignoreMissingApps,
			CacheInvalidateTTL: *tickerTime,
		}
		cachingClient, err = caching.NewCachingBolt(cfClient, config)
		if err != nil {
			log.Fatal("Failed to create boltdb cache", err)
		}
	} else {
		cachingClient = caching.NewCachingEmpty()
	}

	//Creating Events
	events := eventRouting.NewEventRouting(cachingClient, loggingClient)
	err = events.SetupEventRouting(*wantedEvents)
	if err != nil {
		log.Fatal("Error setting up event routing: ", err)
		os.Exit(1)

	}

	//Set extrafields if needed
	events.SetExtraFields(*extraFields)

	//Enable LogsTotalevent
	if *logEventTotals {
		logging.LogStd("Logging total events", true)
		events.LogEventTotals(*logEventTotalsTime)
	}

	if err := cachingClient.Open(); err != nil {
		log.Fatal("Error open cache: ", err)
	}

	KafKaConsumer := kafkaconsumer.NewKafaConsumer(*kfBrokersList, *wantedEvents)
	defer KafKaConsumer.Close()

	// Create a ctx for cancelation signal across the goroutine.
	ctx, cancel := context.WithCancel(context.Background())

	//Let's Start to Consumer
	KafKaConsumer.Consume(ctx)

	//Let's take care of error and Notifications
	go func() {
		for {
			select {
			case err := <-KafKaConsumer.ChErrors:
				fmt.Printf("[ERROR] error from the consumer: %s", err)
			case err := <-KafKaConsumer.ChNotification:
				fmt.Printf("[INFO] Notification from the consumer: %s", err)
			case <-ctx.Done():
				return
			}
		}
	}()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		<-signals
		fmt.Println("Receiving Signal let's stop all goroutine")
		cancel()
	}()

	//Let's handle all message
	go func() {
		for {
			select {
			case enveloppe := <-KafKaConsumer.ChEvents:
				events.RouteEvent(enveloppe)
			case <-ctx.Done():
				return
			}
		}
	}()

	<-ctx.Done()
	defer cancel()
	return 0
}
