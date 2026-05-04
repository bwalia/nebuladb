// Command manager is the NebulaDB Kubernetes operator entrypoint.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	nebulav1alpha1 "github.com/bwalia/nebuladb-operator/api/v1alpha1"
	"github.com/bwalia/nebuladb-operator/internal/controllers"
	"github.com/bwalia/nebuladb-operator/internal/nebulaclient"
	"github.com/bwalia/nebuladb-operator/internal/webhooks"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(nebulav1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr          string
		probeAddr            string
		enableLeaderElection bool
		enableWebhooks       bool
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Prometheus metrics bind")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "healthz/readyz bind")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for high availability of the operator itself.")
	flag.BoolVar(&enableWebhooks, "enable-webhooks", true,
		"Serve admission webhooks. Disable for local development without certs.")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "nebuladb-operator-leader.nebula.nebuladb.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	factory := func(base string) *nebulaclient.Client {
		return nebulaclient.New(base, os.Getenv("NEBULA_OPERATOR_BEARER"))
	}

	if err := (&controllers.NebulaClusterReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		Recorder:      mgr.GetEventRecorderFor("nebulacluster-controller"),
		NebulaFactory: factory,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "NebulaCluster")
		os.Exit(1)
	}
	if err := (&controllers.NebulaBucketReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		Recorder:      mgr.GetEventRecorderFor("nebulabucket-controller"),
		NebulaFactory: factory,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "NebulaBucket")
		os.Exit(1)
	}
	if err := (&controllers.NebulaRebalanceReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		Recorder:      mgr.GetEventRecorderFor("nebularebalance-controller"),
		NebulaFactory: factory,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "NebulaRebalance")
		os.Exit(1)
	}
	if err := (&controllers.NebulaRegionFailoverReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		Recorder:      mgr.GetEventRecorderFor("nebularegionfailover-controller"),
		NebulaFactory: factory,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "NebulaRegionFailover")
		os.Exit(1)
	}

	if enableWebhooks {
		if err := webhooks.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "NebulaCluster")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
