package com.nec.aim.template.entity;

import java.math.BigDecimal;
import java.sql.Blob;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PersonBioMetrics {
	 @Id
	  @GeneratedValue(strategy = GenerationType.AUTO)
	  private Long biometricsId;
	  @Column
	  private String externalId;
	  @Column
	  private Blob biometricData;
	  @Column
	  private Integer size;
	  @Column
	  private BigDecimal containerId;  

}
