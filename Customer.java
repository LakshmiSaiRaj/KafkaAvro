package com.javatechie.dto;

import lombok.Data;

@Data
public class Customer {
    private  int id;
    private String name;
    private String email;
    private String contactNo;

    public Customer(int id, String name, String email, String contactNo) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.contactNo = contactNo;
    }
}
