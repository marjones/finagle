package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag

/**
 * Create custom EventLoopGroup instead of default EpollEventLoopGroup or NioEventLoopGroup.
 * This can be useful to run alternative implementations, e.g. with additional instrumentation.
 * The provided value must be full class name, e.g. io.netty.channel.epoll.EpollEventLoopGroup,
 * which implements io.netty.channel.MultithreadEventLoopGroup
 */
private object epollEventLoopGroupClassName
    extends GlobalFlag[String]("", "Create custom EventLoopGroup")
