package com.iszhaoy.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Optional;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
    private String userId;

    private String useranme;

    private String address;

    private String country;

    private String isocode;

    private String email;

    private String password;

    public User(String email, String password) {
        this.email = email;
        this.password = password;
    }

    private String position;

    public Optional<String> getPosition() {
        return Optional.ofNullable(position);
    }

    public void setPosition(String p) {
        this.position = p;
    }
}
