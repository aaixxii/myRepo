package com.nec.aim.template.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.nec.aim.template.entity.User;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {
}
