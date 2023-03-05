package com.lupo.akkapubsubconnector.model;

public class Message<IN> {
    private IN in;
    private String acknowledgmentId;

    public String getAcknowledgmentId() {
        return acknowledgmentId;
    }

    public void setAcknowledgmentId(String acknowledgmentId) {
        this.acknowledgmentId = acknowledgmentId;
    }

    public IN getIn() {
        return in;
    }

    public void setIn(IN in) {
        this.in = in;
    }

}
