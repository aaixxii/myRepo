package com.nec.aim.client.user.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.nec.aim.client.user.entity.User;


@RequestMapping("/movies")
@RestController
public class MovieController {
  @Autowired
  private RestTemplate restTemplate;

  @GetMapping("/users/{id}")
  public User findById(@PathVariable Long id) {   
    User user = this.restTemplate.getForObject("http://localhost:8000/users/{id}", User.class, id);    
    return user;
  }
}
