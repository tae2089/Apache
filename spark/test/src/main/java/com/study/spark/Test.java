package com.study.spark;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    public static void main(String[] args){

            Pattern pattern = Pattern.compile("^[0-9a-zA-Z]*$"); //영문자만
            String val = "s123"; //대상문자열
	
            Matcher matcher = pattern.matcher(val);
            System.out.println(matcher.find());
    }
}
