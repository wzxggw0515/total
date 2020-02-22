package day.lianxi;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

public class StreamServer {
    public static void main(String[] args) throws IOException {
        ServerSocket socket = new ServerSocket(9999);
        System.out.println("服务器已启动");
        Socket accept = socket.accept();
        System.out.println("客户端以链接....");
        PrintWriter writer = new PrintWriter(accept.getOutputStream());
        Scanner scanner = new Scanner(new FileInputStream("D:\\IDEA\\spark\\Rddexample\\src\\main\\resources\\emp.txt"));
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                String s = scanner.nextLine();
                System.out.println("读取数据："+s);
                writer.println(s);
                writer.flush();
            }
        },1000,5000);

    }
}
