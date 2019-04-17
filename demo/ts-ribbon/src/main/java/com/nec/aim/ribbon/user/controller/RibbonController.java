package com.nec.aim.ribbon.user.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.nec.aim.ribbon.user.entity.User;

/**
 * @author zhouli
 */
@RequestMapping("/movies")
@RestController
public class RibbonController {
  @Autowired
  private RestTemplate restTemplate;

  @GetMapping("/users/{id}")
  public User findById(@PathVariable Long id) {
    // 这里用到了RestTemplate的占位符能力
    User user = this.restTemplate.getForObject(
      "http://microservice-provider-user/users/{id}",
      User.class,
      id
    );
    // ...电影微服务的业务...
    return user;
  }
}
