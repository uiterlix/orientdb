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

import java.util.Set;

import javax.persistence.Version;

/**
 * @author luca.molino
 * 
 */
public class Top {
  @Version
  private Long   version;

  private String value;

  private Sub    sub;
  private Set    subs;

  public Set getSubs() {
    return subs;
  }

  public void setSubs(Set subs) {
    this.subs = subs;
  }

  public Sub getSub() {
    return sub;
  }

  public void setSub(Sub sub) {
    this.sub = sub;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }
}
