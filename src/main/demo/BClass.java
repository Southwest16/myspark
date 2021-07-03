package demo;

public class BClass {
    public void process(ICallback callback) {
        //...
        callback.methodToCallback();
        //...
        System.out.println("ooooooo");
    }
}
