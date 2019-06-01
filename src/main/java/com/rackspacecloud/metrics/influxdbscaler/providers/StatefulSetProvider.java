package com.rackspacecloud.metrics.influxdbscaler.providers;

import com.rackspacecloud.metrics.influxdbscaler.models.StatefulSetStatus;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.models.V1StatefulSet;
import io.kubernetes.client.models.V1StatefulSetStatus;
import io.kubernetes.client.util.Config;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;

@Data
public class StatefulSetProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatefulSetProvider.class);

    private final String namespace;
    private final String statefulSetName;
    private ApiClient apiClient;

    public StatefulSetProvider(
            final String configFileName,
            final String namespace,
            final String statefulSetName) throws IOException {
        this.namespace = namespace;
        this.statefulSetName = statefulSetName;

        if(!StringUtils.isEmpty(configFileName)) {
            apiClient = Config.fromConfig(configFileName);
            Configuration.setDefaultApiClient(apiClient);
        }
        else {
            apiClient = Config.fromCluster();
            Configuration.setDefaultApiClient(apiClient);
        }
    }

    public StatefulSetStatus getStatefulSetStatus() {
        AppsV1Api apiInstance = new AppsV1Api(apiClient);
        String pretty = "true";

        try {
            V1StatefulSet v1StatefulSet =
                    apiInstance.readNamespacedStatefulSetStatus(statefulSetName, namespace, pretty);
            V1StatefulSetStatus status = v1StatefulSet.getStatus();

            return new StatefulSetStatus(
                    status.getCurrentReplicas(), status.getReadyReplicas(),
                    status.getReplicas(),
                    status.getUpdatedReplicas() == null ? 0 : status.getUpdatedReplicas().intValue()
            );
        } catch (ApiException e) {
            LOGGER.error(e.getResponseBody());
            return null;
        }
    }

////    public void getStatefulSetInstances(String namespace, String statefulSetName) {
////        AppsV1Api apiInstance = new AppsV1Api(apiClient);
////        String pretty = "true";
////
////        try {
////            V1StatefulSet result = apiInstance.readNamespacedStatefulSet(
////                    statefulSetName, namespace, pretty, null, null);
////
////            System.out.println(result);
////
////
//////            return result.toString();
////        } catch (ApiException e) {
////            LOGGER.error(e.getMessage(), e);
//////            return e.getLocalizedMessage();
////        }
////    }
//
////    @Override
//    public String call() throws Exception {
//        if(namespace == null || namespace == "") throw new Exception("namespace is null or empty");
//        if(statefulSetName == null || statefulSetName == "") throw new Exception("statefulset statefulSetName is null or empty");
//        if(bodyToPatch == null) throw new Exception("Nothing to patch");
//
//        AppsV1Api apiInstance = new AppsV1Api(apiClient);
//        String pretty = "true";
//
//        try {
//            V1Scale result = apiInstance.patchNamespacedStatefulSetScale(
//                    statefulSetName, namespace, bodyToPatch, pretty, null);
//
//            int statusCheckCount = 0;
//            StatefulSetStatus status = null;
//            while(statusCheckCount < 60) {
//                status = getStatefulSetStatus();
//                if(status.getReadyReplicas() == status.getReplicas()) break;
//
//                Thread.sleep(5000);
//            }
//
//            if(status == null || status.getReadyReplicas() != status.getReplicas())
//                throw new Exception("statefulset nodes are not ready yet or there is no status on that yet");
//
//            return result.toString();
//        } catch (ApiException e) {
//            LOGGER.error(e.getResponseBody());
//            return e.getLocalizedMessage();
//        }
//    }
}
