/*
 *
 * Copyright 2013 Luca Molino (molino.luca--AT--gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.domain.base.transaction;

import java.util.Date;
import java.util.Set;

import javax.persistence.Version;

/**
 * @author luca.molino
 * 
 */
public class Sub {
  @Version
  private Long   version;
  private String test;
  private Set    subsubs;
  private Date   date;
  private Top    top;

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public Set getSubsubs() {
    return subsubs;
  }

  public void setSubsubs(Set subsubs) {
    this.subsubs = subsubs;
  }

  public String getTest() {
    return test;
  }

  public void setTest(String test) {
    this.test = test;
  }

  public Top getTop() {
    return top;
  }

  public void setTop(Top top) {
    this.top = top;
  }
}
