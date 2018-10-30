package dynamicProxy;

import com.alibaba.fastjson.JSONObject;
import sun.misc.ProxyGenerator;

import java.io.FileOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class MyProxy {

    public static void main(String[] args) {
        // 动态代理实例
        Dog dog = new Dog("da huang");

        Animal animal = (Animal) Proxy.newProxyInstance(Dog.class.getClassLoader(), new Class[]{Animal.class}, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                System.out.println("before");
                System.out.println(proxy.getClass());
                System.out.println(method.getName());
                method.invoke(dog, args);
                return null;
            }
        });

        byte[] classFile = ProxyGenerator.generateProxyClass("$Proxy0", Dog.class.getInterfaces());
        String path = "./DogProxy.class";
        try(FileOutputStream fos = new FileOutputStream(path)) {
            fos.write(classFile);
            fos.flush();
            System.out.println("代理类class文件写入成功");
        } catch (Exception e) {
            System.out.println("写文件错误");
        }
        animal.eat("屎");

    }

    public  interface Animal{
        void eat(String food);

        void sleep();
    }

    public static class Dog implements Animal{

        private String name;

        public Dog(String name) {
            this.name = name;
        }

        @Override
        public void eat(String food) {
            System.out.println("狗狗， 喜欢吃！" + food);
        }

        @Override
        public void sleep() {
            System.out.println("狗狗， 白天睡觉，晚上看门！");
        }
    }

}
