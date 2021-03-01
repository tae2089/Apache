package com.study.spark.domain;

import java.math.BigInteger;

@SuppressWarnings("serial")
@Data
@NoArgsConstructor
public class People implements Serializable {
    private String name;
    private Long age;

    @Builder
    public People(String name, Long age) {
        this.name = name;
        this.age = age;
    }

}
