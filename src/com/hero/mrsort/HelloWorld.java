package com.hero.mrsort;
import org.apache.commons.lang.StringUtils;

public class HelloWorld {

	public static void main(String[] args) {
		String myString = "You suck balls.";
		myString = StringUtils.center(myString, 80);
		System.out.println(myString);
	}

}
