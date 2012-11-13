package com.hero.mrsort;

public class MrUtils {

	
	public static long unsignedIntToLong(byte[] b) 
	{
		long l = 0;
	    l |= b[0] & 0xFF;
	    l <<= 8;
	    l |= b[1] & 0xFF;
	    l <<= 8;
	    l |= b[2] & 0xFF;
	    l <<= 8;
	    l |= b[3] & 0xFF;
	    return l;
	}
	
	public static void main(String[] args) throws Exception {

		byte[] bytes = new byte[4];
		
////		00 00 00 0e // 0000000014
//		bytes[0] = 0x00;
//		bytes[1] = 0x00;
//		bytes[2] = 0x00;
//		bytes[3] = 0x0E;
		
//		2d f6 70 80 // 0771125376
		bytes[0] = 0x2D;
		bytes[1] = (byte) 0xF6;
		bytes[2] = 0x70;
		bytes[3] = (byte) 0x80;
		
//		2d f6 70 f2 // 0771125490
		bytes[0] = 0x2D;
		bytes[1] = (byte) 0xF6;
		bytes[2] = 0x70;
		bytes[3] = (byte) 0xF2;
		
//		75 b3 14 dd // 1834095245
		bytes[0] = 0x75;
		bytes[1] = (byte) 0xb3;
		bytes[2] = 0x14;
		bytes[3] = (byte) 0xdd;
		
//		75 b3 15 3e // 1974670654
		bytes[0] = 0x75;
		bytes[1] = (byte) 0xb3;
		bytes[2] = 0x15;
		bytes[3] = (byte) 0x3e;
		
//		a9 d8 e2 5e // 2849563230
		bytes[0] = (byte) 0xa9;
		bytes[1] = (byte) 0xd8;
		bytes[2] = (byte) 0xe2;
		bytes[3] = (byte) 0x5e;
		
//		a9 d8 e2 7b  // 2849563259
		bytes[0] = (byte) 0xa9;
		bytes[1] = (byte) 0xd8;
		bytes[2] = (byte) 0xe2;
		bytes[3] = (byte) 0x7b;

		
//		ff ff ff 64
		bytes[0] = (byte) 0xff;
		bytes[1] = (byte) 0xff;
		bytes[2] = (byte) 0xff;
		bytes[3] = (byte) 0x64;

		
		System.err.println(MrUtils.unsignedIntToLong(bytes));

	}
}
