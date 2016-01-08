package com.razor.springRabbit.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ControllerResponseBuilder<T> {

    T result;
    private boolean success;
    private String errorMsg;
    private String resultTag;
    public static final String JSON_STRING_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS Z";

    public interface ModelBuilder<T> {
        public Map<String, Object> buildModel(ControllerResponseBuilder<T> response);
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getResultTag() {
        return resultTag;
    }

    public void setResultTag(String resultTag) {
        this.resultTag = resultTag;
    }

    public Map<String, Object> generateModel() {
        Map<String, Object> model = new HashMap<String, Object>();
        model.put("success", this.isSuccess() ? "true" : "false");
        if (this.isSuccess()) {
            model.put(this.getResultTag(), this.getResult());
            if(this.getResult() instanceof List) {
                model.put("count", ((List) this.getResult()).size());
            }
        } else {
            model.put("error", this.getErrorMsg());
        }
        return model;
    }

    public String generateJsonResponseString(ModelBuilder modelModifier) {
        Gson gson = new GsonBuilder().setDateFormat(JSON_STRING_FORMAT).create();
        return gson.toJson(modelModifier.buildModel(this));
    }

    public String generateJsonResponseString() {
        Gson gson = new GsonBuilder().setDateFormat(JSON_STRING_FORMAT).create();
        return gson.toJson(this.generateModel());
    }

}