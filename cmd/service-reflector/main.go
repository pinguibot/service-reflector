// Copyright 2026 the Service Reflector authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-logr/logr"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/pflag"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	restclient "k8s.io/client-go/rest"
	v1beta1 "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"

	"github.com/squat/service-reflector/pkg/apiserver"
	"github.com/squat/service-reflector/pkg/controller"
	"github.com/squat/service-reflector/pkg/version"
)

const (
	logLevelAll   = "all"
	logLevelDebug = "debug"
	logLevelInfo  = "info"
	logLevelWarn  = "warn"
	logLevelError = "error"
	logLevelNone  = "none"
)

var availableLogLevels = strings.Join([]string{
	logLevelAll, logLevelDebug, logLevelInfo, logLevelWarn, logLevelError, logLevelNone,
}, ", ")

// urls is a pflag.Value that accumulates parsed URLs.
type urls []*url.URL

func (u *urls) String() string {
	ss := make([]string, len(*u))
	for i, uu := range *u {
		ss[i] = uu.String()
	}
	return strings.Join(ss, ", ")
}

func (u *urls) Set(v string) error {
	for _, raw := range strings.Split(v, ",") {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		uu, err := url.Parse(trimmed)
		if err != nil {
			return err
		}
		*u = append(*u, uu)
	}
	return nil
}

func (u *urls) Type() string { return "url" }

type options struct {
	kubeconfig   string
	clusterID    string
	listen       string
	logLevel     string
	namespace    string
	printVersion bool

	runEmitter bool
	secure     *genericoptions.SecureServingOptionsWithLoopback

	sourceAPIs        urls
	sourceKubeconfigs []string

	// Completed fields
	kitLogger          log.Logger
	logrLogger         logr.Logger
	localConfig        *restclient.Config
	apiserverConfig    *apiserver.Config
	apiserverCompleted apiserver.CompletedConfig
	seInformer         cache.SharedIndexInformer
	esInformer         cache.SharedIndexInformer
}

func newOptions() *options {
	return &options{
		secure: (&genericoptions.SecureServingOptions{
			BindAddress: net.ParseIP("0.0.0.0"),
			BindPort:    6443,
			ServerCert: genericoptions.GeneratableKeyCert{
				PairName:      "emitter",
				CertDirectory: "emitter.local.config/certificates",
			},
		}).WithLoopback(),
	}
}

func (o *options) flagSet() *pflag.FlagSet {
	f := pflag.NewFlagSet("service-reflector", pflag.ExitOnError)
	f.StringVar(&o.kubeconfig, "kubeconfig", "", "Path to kubeconfig for the local cluster.")
	f.StringVar(&o.clusterID, "cluster-id", "", "Name of this cluster (required).")
	f.StringVar(&o.listen, "listen", ":9090", "Address to listen for health and metrics.")
	f.StringVar(&o.logLevel, "log-level", logLevelInfo, fmt.Sprintf("Log level to use. Possible values: %s", availableLogLevels))
	f.StringVar(&o.namespace, "namespace", metav1.NamespaceAll, "Namespace to watch (empty = all).")
	f.BoolVar(&o.printVersion, "version", false, "Print version and exit.")
	f.BoolVar(&o.runEmitter, "emitter", true, "Run the local Emitter API server.")
	f.Var(&o.sourceAPIs, "source-api", "URL of a remote Emitter to watch (repeatable).")
	f.StringArrayVar(&o.sourceKubeconfigs, "source-kubeconfig", nil, "Kubeconfig for a remote cluster (repeatable).")

	emitter := pflag.NewFlagSet("emitter", pflag.ExitOnError)
	emitter.SetNormalizeFunc(func(_ *pflag.FlagSet, name string) pflag.NormalizedName {
		return pflag.NormalizedName("emitter." + name)
	})
	o.secure.AddFlags(emitter)
	f.AddFlagSet(emitter)

	return f
}

func (o *options) complete() []error {
	var errs []error
	errs = append(errs, o.secure.Validate()...)

	// Set up go-kit logger.
	o.kitLogger = log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	o.kitLogger = log.With(o.kitLogger, "ts", log.DefaultTimestampUTC)
	o.kitLogger = log.With(o.kitLogger, "caller", log.DefaultCaller)
	switch o.logLevel {
	case logLevelAll:
		o.kitLogger = level.NewFilter(o.kitLogger, level.AllowAll())
	case logLevelDebug:
		o.kitLogger = level.NewFilter(o.kitLogger, level.AllowDebug())
	case logLevelInfo:
		o.kitLogger = level.NewFilter(o.kitLogger, level.AllowInfo())
	case logLevelWarn:
		o.kitLogger = level.NewFilter(o.kitLogger, level.AllowWarn())
	case logLevelError:
		o.kitLogger = level.NewFilter(o.kitLogger, level.AllowError())
	case logLevelNone:
		o.kitLogger = level.NewFilter(o.kitLogger, level.AllowNone())
	default:
		errs = append(errs, fmt.Errorf("unknown log level %q; possible values: %s", o.logLevel, availableLogLevels))
	}
	o.logrLogger = logr.New(kitLogr{o.kitLogger})

	if o.clusterID == "" {
		errs = append(errs, fmt.Errorf("--cluster-id is required"))
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", o.kubeconfig)
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to create Kubernetes config: %v", err))
		return errs
	}
	o.localConfig = cfg

	if o.runEmitter {
		if err2 := o.completeEmitter(); err2 != nil {
			errs = append(errs, err2)
		}
	}
	return errs
}

func (o *options) completeEmitter() error {
	if err := o.secure.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{net.ParseIP("127.0.0.1")}); err != nil {
		return fmt.Errorf("error creating self-signed certs: %v", err)
	}
	o.apiserverConfig = &apiserver.Config{
		GenericConfig: genericapiserver.NewConfig(apiserver.Codecs),
	}
	if err := o.secure.ApplyTo(
		&o.apiserverConfig.GenericConfig.SecureServing,
		&o.apiserverConfig.GenericConfig.LoopbackClientConfig,
	); err != nil {
		return err
	}

	// Build dynamic informers for the Emitter.
	dynClient, err := dynamic.NewForConfig(o.localConfig)
	if err != nil {
		return fmt.Errorf("creating dynamic client for emitter: %w", err)
	}
	dynFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynClient, 0, o.namespace, nil)

	seGVR := schema.GroupVersionResource{
		Group:    v1beta1.GroupVersion.Group,
		Version:  v1beta1.GroupVersion.Version,
		Resource: "serviceexports",
	}
	esGVR := schema.GroupVersionResource{
		Group:    discoveryv1.SchemeGroupVersion.Group,
		Version:  discoveryv1.SchemeGroupVersion.Version,
		Resource: "endpointslices",
	}
	o.seInformer = dynFactory.ForResource(seGVR).Informer()
	o.esInformer = dynFactory.ForResource(esGVR).Informer()

	o.apiserverCompleted = o.apiserverConfig.Complete(o.seInformer, o.esInformer)
	return nil
}

// Main is the principal function for the binary, wrapped by main for convenience.
func Main() error {
	o := newOptions()
	o.flagSet().Parse(os.Args[1:])

	if o.printVersion {
		fmt.Println(version.Version)
		return nil
	}

	if errs := o.complete(); len(errs) != 0 {
		return multiError(errs)
	}

	r := prometheus.NewRegistry()
	r.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)

	var g run.Group

	if o.runEmitter {
		emitter, err := o.apiserverCompleted.New()
		if err != nil {
			return err
		}

		// Start informers used by the Emitter.
		{
			stop := make(chan struct{})
			g.Add(func() error {
				o.seInformer.Run(stop)
				return nil
			}, func(error) { close(stop) })
		}

		{
			stop := make(chan struct{})
			g.Add(func() error {
				o.kitLogger.Log("msg", "starting emitter on HTTPS")
				return emitter.GenericAPIServer.PrepareRun().Run(stop)
			}, func(error) { close(stop) })
		}
	}

	// Controller manager (reconcilers + watchers).
	{
		ctx, cancel := context.WithCancel(context.Background())
		remoteConfigs, err := buildRemoteConfigs(o)
		if err != nil {
			return err
		}
		g.Add(func() error {
			o.kitLogger.Log("msg", "starting controller manager")
			return controller.Start(ctx, controller.ManagerOptions{
				KubeConfig:    o.localConfig,
				ClusterID:     o.clusterID,
				Namespace:     o.namespace,
				RemoteConfigs: remoteConfigs,
				Log:           o.logrLogger,
			})
		}, func(error) { cancel() })
	}

	// Metrics / health HTTP server.
	{
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		mux.Handle("/metrics", promhttp.HandlerFor(r, promhttp.HandlerOpts{}))
		l, err := net.Listen("tcp", o.listen)
		if err != nil {
			return fmt.Errorf("failed to listen on %s: %v", o.listen, err)
		}
		g.Add(func() error {
			o.kitLogger.Log("msg", "starting metrics server", "addr", o.listen)
			if err2 := http.Serve(l, mux); err2 != nil && err2 != http.ErrServerClosed {
				return fmt.Errorf("metrics server error: %v", err2)
			}
			return nil
		}, func(error) { l.Close() })
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	{
		term := make(chan os.Signal, 1)
		signal.Notify(term, syscall.SIGINT, syscall.SIGTERM)
		cancel := make(chan struct{})
		g.Add(func() error {
			select {
			case <-term:
				o.kitLogger.Log("msg", "received signal; shutting down")
			case <-cancel:
			}
			return nil
		}, func(error) { close(cancel) })
	}

	return g.Run()
}

func main() {
	if err := Main(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

// buildRemoteConfigs builds a map of remoteID -> *rest.Config from CLI flags.
func buildRemoteConfigs(o *options) (map[string]*restclient.Config, error) {
	configs := make(map[string]*restclient.Config)
	for _, u := range o.sourceAPIs {
		cfg, err := clientcmd.BuildConfigFromFlags(u.String(), "")
		if err != nil {
			return nil, fmt.Errorf("building config for remote API %s: %v", u, err)
		}
		configs[u.Host] = cfg
	}
	for _, kc := range o.sourceKubeconfigs {
		cfg, err := clientcmd.BuildConfigFromFlags("", kc)
		if err != nil {
			return nil, fmt.Errorf("building config from kubeconfig %s: %v", kc, err)
		}
		configs[cfg.Host] = cfg
	}
	return configs, nil
}

type multiError []error

func (m multiError) Error() string {
	ss := make([]string, len(m))
	for i, e := range m {
		ss[i] = e.Error()
	}
	return strings.Join(ss, "; ")
}

// kitLogr adapts a go-kit logger to the logr.LogSink interface.
type kitLogr struct{ l log.Logger }

func (k kitLogr) Init(_ logr.RuntimeInfo)                      {}
func (k kitLogr) Enabled(_ int) bool                           { return true }
func (k kitLogr) Info(_ int, msg string, kv ...interface{})    { k.l.Log(append([]interface{}{"msg", msg}, kv...)...) }
func (k kitLogr) Error(err error, msg string, kv ...interface{}) {
	k.l.Log(append([]interface{}{"msg", msg, "error", err}, kv...)...)
}
func (k kitLogr) WithValues(kv ...interface{}) logr.LogSink { return kitLogr{log.With(k.l, kv...)} }
func (k kitLogr) WithName(name string) logr.LogSink {
	return kitLogr{log.With(k.l, "component", name)}
}
