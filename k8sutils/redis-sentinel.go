package k8sutils

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	redisv1beta1 "github.com/OT-CONTAINER-KIT/redis-operator/api/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// RedisSentinelSTS is a interface to call Redis Statefulset function
type RedisSentinelSTS struct {
	RedisStateFulType             string
	ExternalConfig                *string
	Affinity                      *corev1.Affinity `json:"affinity,omitempty"`
	TerminationGracePeriodSeconds *int64           `json:"terminationGracePeriodSeconds,omitempty" protobuf:"varint,4,opt,name=terminationGracePeriodSeconds"`
	ReadinessProbe                *redisv1beta1.Probe
	LivenessProbe                 *redisv1beta1.Probe
}

// RedisSentinelService is a interface to call Redis Service function
type RedisSentinelService struct {
	RedisServiceRole string
}

type RedisReplicationObject struct {
	RedisReplication *redisv1beta1.RedisReplication
}

// Redis Sentinel Create the Redis Sentinel Setup
func CreateRedisSentinel(cr *redisv1beta1.RedisSentinel) error {
	prop := RedisSentinelSTS{
		RedisStateFulType:             "sentinel",
		Affinity:                      cr.Spec.Affinity,
		ReadinessProbe:                cr.Spec.ReadinessProbe,
		LivenessProbe:                 cr.Spec.LivenessProbe,
		TerminationGracePeriodSeconds: cr.Spec.TerminationGracePeriodSeconds,
	}

	if cr.Spec.RedisSentinelConfig.AdditionalSentinelConfig != nil {
		prop.ExternalConfig = cr.Spec.RedisSentinelConfig.AdditionalSentinelConfig
	}

	return prop.CreateRedisSentinelSetup(cr)

}

// Create RedisSentinel Service
func CreateRedisSentinelService(cr *redisv1beta1.RedisSentinel) error {

	prop := RedisSentinelService{
		RedisServiceRole: "sentinel",
	}
	return prop.CreateRedisSentinelService(cr)
}

// Create Redis Sentinel Cluster Setup
func (service RedisSentinelSTS) CreateRedisSentinelSetup(cr *redisv1beta1.RedisSentinel) error {

	stateFulName := cr.ObjectMeta.Name
	logger := statefulSetLogger(cr.Namespace, stateFulName)
	labels := getRedisLabels(stateFulName, "sentinel", service.RedisStateFulType, cr.ObjectMeta.Labels)
	annotations := generateStatefulSetsAnots(cr.ObjectMeta)
	objectMetaInfo := generateObjectMetaInformation(stateFulName, cr.Namespace, labels, annotations)
	err := CreateOrUpdateStateFul(
		cr.Namespace,
		objectMetaInfo,
		generateRedisSentinelParams(cr, service.getSentinelCount(cr), service.ExternalConfig, service.Affinity),
		redisSentinelAsOwner(cr),
		generateRedisSentinelInitContainerParams(cr),
		generateRedisSentinelContainerParams(cr, service.ReadinessProbe, service.LivenessProbe),
		cr.Spec.Sidecars,
	)

	if err != nil {
		logger.Error(err, "Cannot create Sentinel statefulset for Redis")
		return err
	}
	return nil
}

// Create Redis Sentile Params for the statefulset
func generateRedisSentinelParams(cr *redisv1beta1.RedisSentinel, replicas int32, externalConfig *string, affinity *corev1.Affinity) statefulSetParameters {

	res := statefulSetParameters{
		Metadata:                      cr.ObjectMeta,
		Replicas:                      &replicas,
		ClusterMode:                   false,
		NodeSelector:                  cr.Spec.NodeSelector,
		PodSecurityContext:            cr.Spec.PodSecurityContext,
		PodManagementPolicy:           appsv1.ParallelPodManagement,
		PriorityClassName:             cr.Spec.PriorityClassName,
		Affinity:                      affinity,
		TerminationGracePeriodSeconds: cr.Spec.TerminationGracePeriodSeconds,
		Tolerations:                   cr.Spec.Tolerations,
		ServiceAccountName:            cr.Spec.ServiceAccountName,
		UpdateStrategy:                cr.Spec.KubernetesConfig.UpdateStrategy,
	}

	if cr.Spec.KubernetesConfig.ImagePullSecrets != nil {
		res.ImagePullSecrets = cr.Spec.KubernetesConfig.ImagePullSecrets
	}
	if externalConfig != nil {
		res.ExternalConfig = externalConfig
	}
	if cr.Spec.RedisExporter != nil {
		res.EnableMetrics = cr.Spec.RedisExporter.Enabled

	}
	return res
}

// generateRedisSentinelInitContainerParams generates Redis sentinel initcontainer information
func generateRedisSentinelInitContainerParams(cr *redisv1beta1.RedisSentinel) initContainerParameters {

	initcontainerProp := initContainerParameters{}

	if cr.Spec.InitContainer != nil {
		initContainer := cr.Spec.InitContainer

		initcontainerProp = initContainerParameters{
			Enabled:               initContainer.Enabled,
			Role:                  "sentinel",
			Image:                 initContainer.Image,
			ImagePullPolicy:       initContainer.ImagePullPolicy,
			Resources:             initContainer.Resources,
			AdditionalEnvVariable: initContainer.EnvVars,
			Command:               initContainer.Command,
			Arguments:             initContainer.Args,
		}

	}

	return initcontainerProp
}

// Create Redis Sentinel Statefulset Container Params
func generateRedisSentinelContainerParams(cr *redisv1beta1.RedisSentinel, readinessProbeDef *redisv1beta1.Probe, livenessProbeDef *redisv1beta1.Probe) containerParameters {

	trueProperty := true
	falseProperty := false
	containerProp := containerParameters{
		Role:            "sentinel",
		Image:           cr.Spec.KubernetesConfig.Image,
		ImagePullPolicy: cr.Spec.KubernetesConfig.ImagePullPolicy,
		Resources:       cr.Spec.KubernetesConfig.Resources,
		Port: &corev1.ContainerPort{
			Name:          "sentinel",
			ContainerPort: sentinelPort,
		},
		SecurityContext:       cr.Spec.SecurityContext,
		AdditionalEnvVariable: getSentinelEnvVariable(cr),
	}
	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		containerProp.EnabledPassword = &trueProperty
		containerProp.SecretName = cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name
		containerProp.SecretKey = cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key
	} else {
		containerProp.EnabledPassword = &falseProperty
	}
	if cr.Spec.RedisExporter != nil {
		containerProp.RedisExporterImage = cr.Spec.RedisExporter.Image
		containerProp.RedisExporterImagePullPolicy = cr.Spec.RedisExporter.ImagePullPolicy

		if cr.Spec.RedisExporter.Resources != nil {
			containerProp.RedisExporterResources = cr.Spec.RedisExporter.Resources
		}

		if cr.Spec.RedisExporter.EnvVars != nil {
			containerProp.RedisExporterEnv = cr.Spec.RedisExporter.EnvVars
		}
	}
	if readinessProbeDef != nil {
		containerProp.ReadinessProbe = readinessProbeDef
	}
	if livenessProbeDef != nil {
		containerProp.LivenessProbe = livenessProbeDef
	}
	if cr.Spec.TLS != nil {
		containerProp.TLSConfig = cr.Spec.TLS
	}

	return containerProp

}

// Get the Count of the Sentinel
func (service RedisSentinelSTS) getSentinelCount(cr *redisv1beta1.RedisSentinel) int32 {
	return cr.Spec.GetSentinelCounts()
}

// Create the Service for redis sentinel
func (service RedisSentinelService) CreateRedisSentinelService(cr *redisv1beta1.RedisSentinel) error {
	serviceName := cr.ObjectMeta.Name
	logger := serviceLogger(cr.Namespace, serviceName)
	labels := getRedisLabels(serviceName, "sentinel", service.RedisServiceRole, cr.ObjectMeta.Labels)
	annotations := generateServiceAnots(cr.ObjectMeta, nil)

	if cr.Spec.RedisExporter != nil && cr.Spec.RedisExporter.Enabled {
		enableMetrics = true
	} else {
		enableMetrics = false
	}

	objectMetaInfo := generateObjectMetaInformation(serviceName, cr.Namespace, labels, annotations)
	headlessObjectMetaInfo := generateObjectMetaInformation(serviceName+"-headless", cr.Namespace, labels, annotations)

	err := CreateOrUpdateService(cr.Namespace, headlessObjectMetaInfo, redisSentinelAsOwner(cr), false, true, "ClusterIP")
	if err != nil {
		logger.Error(err, "Cannot create headless service for Redis", "Setup.Type", service.RedisServiceRole)
		return err
	}
	err = CreateOrUpdateService(cr.Namespace, objectMetaInfo, redisSentinelAsOwner(cr), enableMetrics, false, "ClusterIP")
	if err != nil {
		logger.Error(err, "Cannot create service for Redis", "Setup.Type", service.RedisServiceRole)
		return err
	}

	// additional service
	additionalServiceName := serviceName + "-additional"
	if cr.Spec.KubernetesConfig.Service != nil {
		additionalServiceAnnotations := cr.Spec.KubernetesConfig.Service.ServiceAnnotations
		additionalObjectMetaInfo := generateObjectMetaInformation(additionalServiceName, cr.Namespace, labels, generateServiceAnots(cr.ObjectMeta, additionalServiceAnnotations))

		additionalServiceType := cr.Spec.KubernetesConfig.Service.ServiceType
		err = CreateOrUpdateService(cr.Namespace, additionalObjectMetaInfo, redisSentinelAsOwner(cr), false, false, additionalServiceType)
		if err != nil {
			logger.Error(err, "Cannot create additional service for Redis", "Setup.Type", service.RedisServiceRole)
			return err
		}
	} else {
		getOpts := v1.GetOptions{
			TypeMeta: generateMetaInformation("Service", "v1"),
		}
		_, err := generateK8sClient().CoreV1().Services(cr.Namespace).Get(context.TODO(), additionalServiceName, getOpts)
		if err == nil || !k8serrors.IsNotFound(err) {
			logger.Info("Delete additional service")
			generateK8sClient().CoreV1().Services(cr.Namespace).Delete(context.TODO(), additionalServiceName, v1.DeleteOptions{})
		}
	}

	return nil
}

func getSentinelEnvVariable(cr *redisv1beta1.RedisSentinel) *[]corev1.EnvVar {
	envVar := &[]corev1.EnvVar{
		{
			Name:  "REDIS_MASTER_HOST",
			Value: "127.0.0.1",
		},
		{
			Name:  "REDIS_SENTINEL_QUORUM",
			Value: getSentinelQuorum(&cr.Spec),
		},
		{
			Name:  "REDIS_SENTINEL_DOWN_AFTER_MILLISECONDS",
			Value: cr.Spec.RedisSentinelConfig.DownAfterMilliseconds,
		},
		{
			Name:  "REDIS_SENTINEL_FAILOVER_TIMEOUT",
			Value: cr.Spec.RedisSentinelConfig.FailoverTimeout,
		},
	}

	return envVar

}

func getRedisReplicationMasterIP(cr *redisv1beta1.RedisSentinel, replicationName string) string {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)

	// replicationName := cr.Spec.RedisSentinelConfig.RedisReplicationName
	replicationNamespace := cr.Namespace

	var replicationInstance redisv1beta1.RedisReplication
	var realMasterPod string

	// Get Request on Dynamic Client
	customObject, err := generateK8sDynamicClient().Resource(schema.GroupVersionResource{
		Group:    "redis.redis.opstreelabs.in",
		Version:  "v1beta1",
		Resource: "redisreplications",
	}).Namespace(replicationNamespace).Get(context.TODO(), replicationName, v1.GetOptions{})

	if err != nil {
		logger.Error(err, "Failed to Execute Get Request", "replication name", replicationName, "namespace", replicationNamespace)
		return ""
	} else {
		logger.Info("Successfully Execute the Get Request", "replication name", replicationName, "namespace", replicationNamespace)
	}

	// Marshal CustomObject to JSON
	replicationJSON, err := customObject.MarshalJSON()
	if err != nil {
		logger.Error(err, "Failed To Load JSON")
		return ""
	}

	// Unmarshal The JSON on Object
	if err := json.Unmarshal(replicationJSON, &replicationInstance); err != nil {
		logger.Error(err, "Failed To Unmarshal JSON over the Object")
		return ""
	}

	masterPods := GetRedisNodesByRole(&replicationInstance, "master")

	if len(masterPods) == 0 {
		realMasterPod = ""
		err := errors.New("no master pods found")
		logger.Error(err, "")
	} else if len(masterPods) == 1 {
		realMasterPod = masterPods[0]
	} else {
		realMasterPod = checkAttachedSlave(&replicationInstance, masterPods)
	}

	realMasterInfo := RedisDetails{
		PodName:   realMasterPod,
		Namespace: replicationNamespace,
	}

	realMasterPodIP := getRedisServerIP(realMasterInfo)
	return realMasterPodIP
}

func getSentinelQuorum(spec *redisv1beta1.RedisSentinelSpec) string {
	if spec.RedisSentinelConfig.Quorum == "" {
		return strconv.Itoa(int(*spec.Size/2 + 1))
	} else {
		return spec.RedisSentinelConfig.Quorum
	}
}
