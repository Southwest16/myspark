package demo;

import java.util.BitSet;

public class JavaDemo {
    public static void main(String[] args) {
        BitSet bitSet = new BitSet();
        bitSet.set(-2051216335);
        bitSet.set(1);
        bitSet.set(1);
        bitSet.set(2);
        bitSet.set(3);
        int len = bitSet.length();
        System.out.println(len);
        for (int i = 0; i < len; i++) {
            System.out.println(bitSet.get(i));
        }

        System.out.println("192.168.10.78".hashCode());
    }
}
