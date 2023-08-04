package controllers

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/OT-CONTAINER-KIT/redis-operator/k8sutils"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1beta1 "github.com/OT-CONTAINER-KIT/redis-operator/api/v1beta1"
)

// RedisSentinelReconciler reconciles a RedisSentinel object
type RedisSentinelReconciler struct {
	client.Client
	Log           logr.Logger
	Scheme        *runtime.Scheme
	SentinelReady atomic.Bool
}

// Reconcile is part of the main kubernetes reconciliation loop which aims
func (r *RedisSentinelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reqLogger := r.Log.WithValues("Request.Namespace", req.Namespace, "Request.Name", req.Name)
	reqLogger.Info("Reconciling opstree redis controller")
	instance := &redisv1beta1.RedisSentinel{}

	sentinelReady := false
	defer func() {
		if r.SentinelReady.Load() != sentinelReady {
			reqLogger.Info("Sentinel status ready: " + strconv.FormatBool(sentinelReady))
			r.SentinelReady.Store(sentinelReady)
		}
	}()

	err := r.Client.Get(context.TODO(), req.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	// Get total Sentinel Replicas
	// sentinelReplicas := instance.Spec.GetSentinelCounts("sentinel")

	if err := k8sutils.HandleRedisSentinelFinalizer(instance, r.Client); err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	if err := k8sutils.AddRedisSentinelFinalizer(instance, r.Client); err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	// Create Redis Sentinel
	err = k8sutils.CreateRedisSentinel(instance)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Create the Service for Redis Sentinel
	if err = k8sutils.CreateRedisSentinelService(instance); err != nil {
		return ctrl.Result{}, err
	}

	// check sentinel ready
	sts := &appsv1.StatefulSet{}
	if err = r.Get(ctx, req.NamespacedName, sts); err != nil {
		return ctrl.Result{}, err
	}
	reqLogger.Info(fmt.Sprintf("sts.spec=%d, availables=%d", *sts.Spec.Replicas, sts.Status.AvailableReplicas))
	if *sts.Spec.Replicas == sts.Status.AvailableReplicas && sts.Status.AvailableReplicas > 0 {
		sentinelReady = true
	}

	reqLogger.Info("Will reconcile redis operator in again 10 seconds")
	return ctrl.Result{RequeueAfter: time.Second * 10}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisSentinelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1beta1.RedisSentinel{}).
		Complete(r)
}
