package com.haoxiang.kafka.pojo;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName User.java
 * @Description TODO
 * @createTime 2021年01月19日 22:10:00
 */
public class User {
    private String firstName;
    private String lastName;
    private int age;
    private String address;

    public User(String firstName, String lastName, int age, String address) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.address = address;
    }

    @Override
    public String toString() {
        return "User{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", age=" + age +
                ", address='" + address + '\'' +
                '}';
    }
}
