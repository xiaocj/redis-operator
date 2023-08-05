package k8sutils

import (
	"context"

	redisv1beta1 "github.com/OT-CONTAINER-KIT/redis-operator/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreateReplicationService method will create replication service for Redis
func CreateReplicationService(cr *redisv1beta1.RedisReplication) error {
	logger := serviceLogger(cr.Namespace, cr.ObjectMeta.Name)
	labels := getRedisLabels(cr.ObjectMeta.Name, "replication", "replication", cr.ObjectMeta.Labels)
	annotations := generateServiceAnots(cr.ObjectMeta, nil)
	if cr.Spec.RedisExporter != nil && cr.Spec.RedisExporter.Enabled {
		enableMetrics = true
	} else {
		enableMetrics = false
	}

	headlessObjectMetaInfo := generateObjectMetaInformation(cr.ObjectMeta.Name+"-headless", cr.Namespace, labels, annotations)
	err := CreateOrUpdateService(cr.Namespace, headlessObjectMetaInfo, redisReplicationAsOwner(cr), false, true, "ClusterIP")
	if err != nil {
		logger.Error(err, "Cannot create replication headless service for Redis")
		return err
	}
	// objectMetaInfo := generateObjectMetaInformation(cr.ObjectMeta.Name, cr.Namespace, labels, annotations)
	// err = CreateOrUpdateService(cr.Namespace, objectMetaInfo, redisReplicationAsOwner(cr), enableMetrics, false, "ClusterIP")
	// if err != nil {
	// 	logger.Error(err, "Cannot create replication service for Redis")
	// 	return err
	// }

	// additional service
	additionalServiceName := cr.ObjectMeta.Name + "-additional"
	if cr.Spec.KubernetesConfig.Service != nil {
		additionalServiceType := "ClusterIP"
		additionalServiceType = cr.Spec.KubernetesConfig.Service.ServiceType
		additionalServiceAnnotations := cr.Spec.KubernetesConfig.Service.ServiceAnnotations
		additionalObjectMetaInfo := generateObjectMetaInformation(additionalServiceName, cr.Namespace, labels, generateServiceAnots(cr.ObjectMeta, additionalServiceAnnotations))
		err = CreateOrUpdateService(cr.Namespace, additionalObjectMetaInfo, redisReplicationAsOwner(cr), false, false, additionalServiceType)
		if err != nil {
			logger.Error(err, "Cannot create additional service for Redis Replication")
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

// CreateReplicationRedis will create a replication redis setup
func CreateReplicationRedis(cr *redisv1beta1.RedisReplication) error {
	stateFulName := cr.ObjectMeta.Name
	logger := statefulSetLogger(cr.Namespace, cr.ObjectMeta.Name)
	labels := getRedisLabels(cr.ObjectMeta.Name, "replication", "replication", cr.ObjectMeta.Labels)
	annotations := generateStatefulSetsAnots(cr.ObjectMeta)
	objectMetaInfo := generateObjectMetaInformation(stateFulName, cr.Namespace, labels, annotations)
	err := CreateOrUpdateStateFul(cr.Namespace,
		objectMetaInfo,
		generateRedisReplicationParams(cr),
		redisReplicationAsOwner(cr),
		generateRedisReplicationInitContainerParams(cr),
		generateRedisReplicationContainerParams(cr),
		cr.Spec.Sidecars,
	)
	if err != nil {
		logger.Error(err, "Cannot create replication statefulset for Redis")
		return err
	}
	return nil
}

func GetRedisPassword(cr *redisv1beta1.RedisReplication) (string, error) {
	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		return getRedisPassword(cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
	}

	return "", nil
}

func generateRedisReplicationParams(cr *redisv1beta1.RedisReplication) statefulSetParameters {
	replicas := cr.Spec.GetReplicationCounts()
	res := statefulSetParameters{
		Replicas:                      &replicas,
		ClusterMode:                   false,
		NodeSelector:                  cr.Spec.NodeSelector,
		PodSecurityContext:            cr.Spec.PodSecurityContext,
		PriorityClassName:             cr.Spec.PriorityClassName,
		Affinity:                      cr.Spec.Affinity,
		Tolerations:                   cr.Spec.Tolerations,
		TerminationGracePeriodSeconds: cr.Spec.TerminationGracePeriodSeconds,
		UpdateStrategy:                cr.Spec.KubernetesConfig.UpdateStrategy,
	}
	if cr.Spec.KubernetesConfig.ImagePullSecrets != nil {
		res.ImagePullSecrets = cr.Spec.KubernetesConfig.ImagePullSecrets
	}
	if cr.Spec.Storage != nil {
		res.PersistentVolumeClaim = cr.Spec.Storage.VolumeClaimTemplate
	}
	if cr.Spec.RedisConfig != nil {
		res.ExternalConfig = cr.Spec.RedisConfig.AdditionalRedisConfig
	}
	if cr.Spec.RedisExporter != nil {
		res.EnableMetrics = cr.Spec.RedisExporter.Enabled
	}
	if cr.Spec.ServiceAccountName != nil {
		res.ServiceAccountName = cr.Spec.ServiceAccountName
	}
	return res
}

// generateRedisReplicationContainerParams generates Redis container information
func generateRedisReplicationContainerParams(cr *redisv1beta1.RedisReplication) containerParameters {
	trueProperty := true
	falseProperty := false
	containerProp := containerParameters{
		Role:            "replication",
		Image:           cr.Spec.KubernetesConfig.Image,
		ImagePullPolicy: cr.Spec.KubernetesConfig.ImagePullPolicy,
		Resources:       cr.Spec.KubernetesConfig.Resources,
		Port: corev1.ContainerPort{
			Name:          "replication",
			ContainerPort: RedisPort,
		},
		SecurityContext: cr.Spec.SecurityContext,
	}

	if cr.Spec.Storage != nil {
		containerProp.AdditionalVolume = cr.Spec.Storage.VolumeMount.Volume
		containerProp.AdditionalMountPath = cr.Spec.Storage.VolumeMount.MountPath
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
	if cr.Spec.ReadinessProbe != nil {
		containerProp.ReadinessProbe = cr.Spec.ReadinessProbe
	}
	if cr.Spec.LivenessProbe != nil {
		containerProp.LivenessProbe = cr.Spec.LivenessProbe
	}
	if cr.Spec.Storage != nil {
		containerProp.PersistenceEnabled = &trueProperty
	}
	if cr.Spec.TLS != nil {
		containerProp.TLSConfig = cr.Spec.TLS
	}
	if cr.Spec.ACL != nil {
		containerProp.ACLConfig = cr.Spec.ACL
	}
	return containerProp
}

// generateRedisReplicationInitContainerParams generates Redis Replication initcontainer information
func generateRedisReplicationInitContainerParams(cr *redisv1beta1.RedisReplication) initContainerParameters {
	trueProperty := true
	initcontainerProp := initContainerParameters{}

	if cr.Spec.InitContainer != nil {
		initContainer := cr.Spec.InitContainer

		initcontainerProp = initContainerParameters{
			Enabled:               initContainer.Enabled,
			Role:                  "replication",
			Image:                 initContainer.Image,
			ImagePullPolicy:       initContainer.ImagePullPolicy,
			Resources:             initContainer.Resources,
			AdditionalEnvVariable: initContainer.EnvVars,
			Command:               initContainer.Command,
			Arguments:             initContainer.Args,
		}

		if cr.Spec.Storage != nil {
			initcontainerProp.AdditionalVolume = cr.Spec.Storage.VolumeMount.Volume
			initcontainerProp.AdditionalMountPath = cr.Spec.Storage.VolumeMount.MountPath
		}
		if cr.Spec.Storage != nil {
			initcontainerProp.PersistenceEnabled = &trueProperty
		}

	}

	return initcontainerProp
}
