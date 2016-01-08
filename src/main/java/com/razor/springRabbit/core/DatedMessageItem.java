package com.razor.springRabbit.core;

import java.util.Date;

public class DatedMessageItem implements Comparable<DatedMessageItem>{
    private Date addedDate;
    private String message;

    public DatedMessageItem(String msg) {
        this.addedDate = new Date();
        this.message = msg;
    }

    public Date getAddedDate()
    {
        return addedDate;
    }

    public String getMessage()
    {
        return message;
    }

    @Override
    public int compareTo(DatedMessageItem rhs) {
        if (this == rhs) {
            return 0;
        }
        return this.getAddedDate().compareTo(rhs.getAddedDate());
    }
}
