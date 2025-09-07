package com.anz.fastpayment.sender.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WrapperRequest {
    @JsonProperty("Body")
    private Body body;
    @JsonProperty("Trailer")
    private Trailer trailer;

    public Body getBody() { return body; }
    public void setBody(Body body) { this.body = body; }
    public Trailer getTrailer() { return trailer; }
    public void setTrailer(Trailer trailer) { this.trailer = trailer; }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Body {
        @JsonProperty("PmtAddRq")
        private List<Object> pmtAddRq;
        public List<Object> getPmtAddRq() { return pmtAddRq; }
        public void setPmtAddRq(List<Object> pmtAddRq) { this.pmtAddRq = pmtAddRq; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Trailer {
        @JsonProperty("ServiceStatus")
        private ServiceStatus serviceStatus;
        @JsonProperty("ResHdr")
        private ResHdr resHdr;
        public ServiceStatus getServiceStatus() { return serviceStatus; }
        public void setServiceStatus(ServiceStatus serviceStatus) { this.serviceStatus = serviceStatus; }
        public ResHdr getResHdr() { return resHdr; }
        public void setResHdr(ResHdr resHdr) { this.resHdr = resHdr; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ServiceStatus {
        @JsonProperty("StatusCode")
        private String statusCode;
        @JsonProperty("StatusDesc")
        private String statusDesc;
        public String getStatusCode() { return statusCode; }
        public void setStatusCode(String statusCode) { this.statusCode = statusCode; }
        public String getStatusDesc() { return statusDesc; }
        public void setStatusDesc(String statusDesc) { this.statusDesc = statusDesc; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ResHdr {
        @JsonProperty("Map")
        private List<MapEntry> map;
        public List<MapEntry> getMap() { return map; }
        public void setMap(List<MapEntry> map) { this.map = map; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MapEntry {
        @JsonProperty("MapKey")
        private String mapKey;
        @JsonProperty("MapValue")
        private String mapValue;
        public String getMapKey() { return mapKey; }
        public void setMapKey(String mapKey) { this.mapKey = mapKey; }
        public String getMapValue() { return mapValue; }
        public void setMapValue(String mapValue) { this.mapValue = mapValue; }
    }
}


