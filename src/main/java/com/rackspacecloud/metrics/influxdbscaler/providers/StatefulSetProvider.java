package com.rackspacecloud.metrics.influxdbscaler.providers;

import com.rackspacecloud.metrics.influxdbscaler.models.PatchStatefulSetInput;
import com.rackspacecloud.metrics.influxdbscaler.models.StatefulSetStatus;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.models.V1Scale;
import io.kubernetes.client.models.V1StatefulSet;
import io.kubernetes.client.models.V1StatefulSetStatus;
import io.kubernetes.client.util.Config;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.Callable;

@Service
@Data
public class StatefulSetProvider implements Callable<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatefulSetProvider.class);

    private String namespace;
    private String name;
    private PatchStatefulSetInput[] bodyToPatch;
    private ApiClient apiClient;

    public StatefulSetProvider() throws IOException {
        initialize();
    }

    private void initialize() throws IOException {
        apiClient = Config.fromConfig("/Users/mrit1806/.kube/config");
        Configuration.setDefaultApiClient(apiClient);

////        ApiClient defaultClient = Configuration.getDefaultApiClient();
//        ApiClient defaultClient = Config.defaultClient();
//        defaultClient.setBasePath("https://35.232.10.27");
    }

    public StatefulSetStatus getStatefulSetStatus(final String namespace, final String name) {
        AppsV1Api apiInstance = new AppsV1Api(apiClient);
        String pretty = "true";

        try {
            V1StatefulSet v1StatefulSet = apiInstance.readNamespacedStatefulSetStatus(name, namespace, pretty);
            V1StatefulSetStatus status = v1StatefulSet.getStatus();

            return new StatefulSetStatus(
                    status.getCurrentReplicas(), status.getReadyReplicas(),
                    status.getReplicas(),
                    status.getUpdatedReplicas() == null ? 0 : status.getUpdatedReplicas().intValue()
            );
        } catch (ApiException e) {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public String call() throws Exception {
        if(namespace == null || namespace == "") throw new Exception("namespace is null or empty");
        if(name == null || name == "") throw new Exception("statefulset name is null or empty");
        if(bodyToPatch == null) throw new Exception("Nothing to patch");

        AppsV1Api apiInstance = new AppsV1Api(apiClient);
        String pretty = "true";

        try {
            V1Scale result = apiInstance.patchNamespacedStatefulSetScale(
                    name, namespace, bodyToPatch, pretty, null);

            int statusCheckCount = 0;
            StatefulSetStatus status = null;
            while(statusCheckCount < 30) {
                status = getStatefulSetStatus(namespace, name);
                if(status.getReadyReplicas() == status.getReplicas()) break;

                Thread.sleep(1000);
            }

            if(status == null || status.getReadyReplicas() != status.getReplicas())
                throw new Exception("statefulset nodes are not ready yet or there is no status on that yet");

            return result.toString();
        } catch (ApiException e) {
            LOGGER.error(e.getMessage(), e);
            return e.getLocalizedMessage();
        }
    }
}
