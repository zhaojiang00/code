package com.zj.mq;

import com.alibaba.fastjson.JSONObject;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.charset.StandardCharsets;

/**
 * @description: 消息生产者
 * @author: zhaojiang
 * @email: 2201838453@qq.com
 */
public class Producer {
    private void bind(String host,int port){
        EventLoopGroup workGroup=new NioEventLoopGroup();
        Bootstrap bootstrap=new Bootstrap();
        try {
            bootstrap.group(workGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ProducerChildChannelHandler());
            ChannelFuture channelFuture = bootstrap.connect(host, port).sync();
            channelFuture.channel().closeFuture().sync();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            workGroup.shutdownGracefully();
        }
    }

    private static class ProducerChildChannelHandler extends ChannelInitializer<SocketChannel>{
        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            socketChannel.pipeline().addLast(new ProducerHandler());
        }
    }

    private static class ProducerHandler extends ChannelInboundHandlerAdapter{
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            JSONObject data = new JSONObject();
            data.put("type", "producer");
            JSONObject msg = new JSONObject();
            msg.put("username", "zhaojiang");
            msg.put("password", "123456");
            data.put("msg", msg);
            System.out.println("消息生产成功");
            byte[] req = data.toJSONString().getBytes();
            ByteBuf firstMSG = Unpooled.buffer(req.length);
            firstMSG.writeBytes(req);
            ctx.writeAndFlush(firstMSG);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf buf = (ByteBuf) msg;
            byte[] req = new byte[buf.readableBytes()];
            buf.readBytes(req);
            String body = new String(req, StandardCharsets.UTF_8);
            System.out.println("客户端接收到服务器端请求:" + body);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ctx.close();
        }
    }

    public static void main(String[] args) {
        int port=10000;
        String host="127.0.0.1";
        Producer producer=new Producer();
        producer.bind(host,port);
    }
}
