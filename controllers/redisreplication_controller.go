package controllers

import (
	"context"
	"strconv"
	"time"

	"github.com/OT-CONTAINER-KIT/redis-operator/k8sutils"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1beta1 "github.com/OT-CONTAINER-KIT/redis-operator/api/v1beta1"
	rediscli "github.com/OT-CONTAINER-KIT/redis-operator/utils/redis"
	corev1 "k8s.io/api/core/v1"
)

// RedisReplicationReconciler reconciles a RedisReplication object
type RedisReplicationReconciler struct {
	client.Client
	Log                logr.Logger
	Scheme             *runtime.Scheme
	SentinelReconciler *RedisSentinelReconciler
	RedisCli           rediscli.RedisClient
	SentinelCli        rediscli.SentinelClient
}

func (r *RedisReplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	reqLogger := r.Log.WithValues("Request.Namespace", req.Namespace, "Request.Name", req.Name)
	reqLogger.Info("Reconciling opstree redis replication controller")
	instance := &redisv1beta1.RedisReplication{}

	err := r.Client.Get(context.TODO(), req.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !r.SentinelReconciler.SentinelReady.Load() {
		reqLogger.Info("Sentinel is not ready, so skipping reconcile")
		return ctrl.Result{RequeueAfter: time.Second * 10}, nil
	}

	if _, found := instance.ObjectMeta.GetAnnotations()["redisreplication.opstreelabs.in/skip-reconcile"]; found {
		reqLogger.Info("Found annotations redisreplication.opstreelabs.in/skip-reconcile, so skipping reconcile")
		return ctrl.Result{RequeueAfter: time.Second * 10}, nil
	}

	if err := k8sutils.HandleRedisReplicationFinalizer(instance, r.Client); err != nil {
		return ctrl.Result{}, err
	}

	if err := k8sutils.AddRedisReplicationFinalizer(instance, r.Client); err != nil {
		return ctrl.Result{}, err
	}

	err = k8sutils.CreateReplicationRedis(instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	err = k8sutils.CreateReplicationService(instance)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Set Pod distruptiuon Budget Later

	redisReplicationInfo, err := k8sutils.GetStatefulSet(instance.Namespace, instance.ObjectMeta.Name)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	// Check that the Leader and Follower are ready in redis replication
	if redisReplicationInfo.Status.ReadyReplicas != *instance.Spec.Size {
		reqLogger.Info("Redis leader and follower nodes are not ready yet", "Ready.Replicas", strconv.Itoa(int(redisReplicationInfo.Status.ReadyReplicas)), "Expected.Replicas", *instance.Spec.Size)
		return ctrl.Result{RequeueAfter: time.Second * 60}, nil
	}

	reqLogger.Info("Creating redis replication by executing replication creation commands", "Replication.Ready", strconv.Itoa(int(redisReplicationInfo.Status.ReadyReplicas)))

	redisPods, err := k8sutils.GetStatefulSetPods(redisReplicationInfo)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	redisPass, err := k8sutils.GetRedisPassword(instance)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	masterPods := []*corev1.Pod{}
	for index := range redisPods.Items {
		pod := &redisPods.Items[index]

		options := rediscli.NewRedisClientOptions(&pod.Status.PodIP, k8sutils.RedisPort, redisPass)
		if isMaster, err := r.RedisCli.IsMaster(options); err != nil {
			reqLogger.Error(err, "Redis client failed", "name", pod.GetName(), "IP", pod.Status.PodIP)
			return ctrl.Result{RequeueAfter: time.Second * 60}, err
		} else {
			if isMaster {
				masterPods = append(masterPods, pod)
			}
		}
	}

	if len(masterPods) == 1 {
		// do nothing
	} else if len(masterPods) == 0 {

		// // retry
		// for _, pod := range redisPods.Items {
		// 	options := rediscli.NewRedisClientOptions(&pod.Status.PodIP, redisPort, redisPass)

		// 	if isMaster, err := r.redisCli.IsMaster(options); err != nil {
		// 		reqLogger.Error(err, "Redis client failed", "name", pod.GetName(), "IP", pod.Status.PodIP)
		// 		return ctrl.Result{RequeueAfter: time.Second * 60}, err
		// 	} else {
		// 		if isMaster {
		// 			masterPods = append(masterPods, pod)
		// 		}
		// 	}
		// }
	} else {
		reqLogger.Info("To many master redis node!!", "masterNum=", len(masterPods))
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	// leaderReplicas := int32(1)
	// followerReplicas := instance.Spec.GetReplicationCounts() - leaderReplicas
	// totalReplicas := leaderReplicas + followerReplicas

	// if len(k8sutils.GetRedisNodesByRole(instance, "master")) > int(leaderReplicas) {
	// 	masterNodes := k8sutils.GetRedisNodesByRole(instance, "master")
	// 	slaveNodes := k8sutils.GetRedisNodesByRole(instance, "slave")
	// 	err := k8sutils.CreateMasterSlaveReplication(instance, masterNodes, slaveNodes)
	// 	if err != nil {
	// 		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	// 	}
	// }

	reqLogger.Info("Will reconcile redis operator in again 10 seconds")
	return ctrl.Result{RequeueAfter: time.Second * 10}, nil

}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisReplicationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1beta1.RedisReplication{}).
		Complete(r)
}
