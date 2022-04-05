package com.zj.mq;

import com.alibaba.fastjson.JSONObject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @description: 消息队列Server端
 * @author: zhaojiang
 * @email: 2201838453@qq.com
 */
public class MQServer {

    private static final String type_of_consumer="consumer";
    private static final String type_of_producer="producer";

    private static final LinkedBlockingDeque<String> msg_queue=new LinkedBlockingDeque<>();
    private static final List<ChannelHandlerContext> consumerChannelList=new ArrayList<>();

    private void bind(int port){
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workGroup = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap();
        try {
            bootstrap.group(bossGroup, workGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 200)
                    .childHandler(new ChildChannelHandler());
            ChannelFuture future = bootstrap.bind(port).sync();
            System.out.println("MQ启动成功");
            future.channel().closeFuture().sync();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
        }
    }
    private class ChildChannelHandler extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            socketChannel.pipeline().addLast(new MQServerHandler());
        }
    }

    public class MQServerHandler extends SimpleChannelInboundHandler<Object>{
        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
            String body = byteBufferToString(o);
            JSONObject jsonObject = JSONObject.parseObject(body);
            String type = jsonObject.getString("type");
            switch (type) {
                case type_of_consumer:
                    consumer(channelHandlerContext);
                    break;
                case type_of_producer:
                    String msg = jsonObject.getString("msg");
                    producer(msg);
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
           ctx.close();
        }
    }

    private String byteBufferToString(Object msg) throws UnsupportedEncodingException {
        if (msg == null) {
            return null;
        }
        ByteBuf buf = (ByteBuf) msg;
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        return new String(req, StandardCharsets.UTF_8);
    }

    private void consumer(ChannelHandlerContext channelHandlerContext){
        consumerChannelList.add(channelHandlerContext);
        String poll = msg_queue.poll();
        if (!StringUtils.isEmpty(poll)) {
            ByteBuf resp = Unpooled.copiedBuffer(poll.getBytes());
            channelHandlerContext.writeAndFlush(resp);
        }
    }

    private void producer(String msg){
        msg_queue.offer(msg);
        //可做负载均衡
        consumerChannelList.forEach((channelHandlerContext) -> {
            String poll = msg_queue.poll();
            if (StringUtils.isEmpty(poll)) {
                return;
            }
            ByteBuf resp = Unpooled.copiedBuffer(poll.getBytes());
            channelHandlerContext.writeAndFlush(resp);
        });
    }

    public static void main(String[] args) {
        int port=10000;
        MQServer mqServer=new MQServer();
        mqServer.bind(port);
    }
}
