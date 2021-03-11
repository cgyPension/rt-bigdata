package com.realtime.bean;



/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/30 10:23
 */

public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;

    public TxEvent() {
    }


    public TxEvent(String txId, String payChannel, Long eventTime) {
        this.txId = txId;
        this.payChannel = payChannel;
        this.eventTime = eventTime;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getPayChannel() {
        return payChannel;
    }

    public void setPayChannel(String payChannel) {
        this.payChannel = payChannel;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "TxEvent{" +
                "txId='" + txId + '\'' +
                ", payChannel='" + payChannel + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

}
