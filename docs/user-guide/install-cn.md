# 概述 #

感谢使用联科并行文件系统（CPFS）开源版。CPFS 是具有容错机制，安装简单
的高性能分布式并行文件系统。 CPFS 高度兼容 POSIX 标准，因此，绝大多数
应用程序毋须改动就可以通过 CPFS 存取数据。下面我们说明 CPFS 的安装过程。
我们先解释一些基本概念。

CPFS 集群由三个模块组成：

*DS（数据服务器）*
: DS 是一个负责把实际文件系统数据存储到本地目录中的守护进程（daemon
process）。每 5 台 DS 组织成一个 DS 组（编译时的常数，设定在
`cpfs/common.hpp` 中的 `kNumDSPerGroup`），一套 CPFS 可以有一个或多个
DS 组。如果一个 DS 组中的一台 DS 丢失，该 DS 组以降级模式继续运行，就
像一个 RAID-5 磁盘阵列在丢失一块盘的情况下继续工作一样。如果故障的 DS
重新启动，它会自动与同组的其他 DS 同步修改过的档案中的内容。如果使用新
的 DS，则所有档案均会被同步。在同步过程中，文件系统请求排队等待同步完
成，不会响应请求。

这有两个结果。首先，DS 丢失时，只会短时间服务器不响应。 但是当重新启动
DS 时，无响应时间取决于要同步的数据量：同步的数据量越大，中断的时间越
长。

其次，同步的数据量取决于要同步的文件的总大小。 如果文件相对较小，只有
几个文件被修改，则中断时间很短。如果许多文件被修改，并且它们很大，那么
中断可能很长。

*MS（元数据服务器）*
: 元数据服务器是一个负责把元数据存储到本地目录的守护进程（daemon
process）。一个 CPFS 1.0 系统最多支持同时运行两个 MS，提供容错机制。在
任何时候，一个 MS 处于激活状态，另外一个（如果有的话）备用。如果备用的
MS 发现激活状态的 MS 丢失，而 DS 试图连接它，它将接管 DS 的连接请求并
自动成为激活的 MS。与 DS 类似，重新上线的 MS 会自动与在线的 MS 同步，
系统在同步时不响应请求。

*FC（文件系统客户端，挂载点）*
: 文件系统客户端是一个基于 FUSE 的守护进程，当本地的其他程序访问挂载点
的时候，FC 负责联络 CPFS 服务器，在挂载点上提供 POSIX 文件系统接口。

下面是一个简单的 CPFS 网络的例子。

![](network-topology.png)

# 规划 CPFS 集群 #

## 硬件 ##

CPFS 支持各种块存储设备和 x86 机器。为获得最佳性能和容错性，建议 MS 、
DS 和 FC 分别在不同的服务器上运行。运行 MS 和 DS 的服务器至少每台配置
4 个 CPU 核和 4GB 内存。虽然我们不太建议，但在同一伺服器同时运行 MS 和
DS 是可行的。

## 操作系统 ##

CPFS 可运行在近期的 64 位 CentOS / RHEL，Fedora，Ubuntu 或 Debian
Linux 发行版之上。构建脚本已准备好，可使用 Docker 为每个发行版创建合适
的二进制代码。这些构建脚本非常简单，修改用作其他发行版也相对容易。

## 存储 ##

为了简化磁盘空间管理，我们建议为每个 MS 和 DS 分配一个专用的硬盘分区作
为本地目录。这些分区最好使用 XFS，并使用 `-n ftype=1` 选项进行格式化
(如果你的 `mkfs.xfs` 支持这个选项)， 但也可使用 ext4 或其他支援扩展属性的
文件系统。由于 DS 采用类似 RAID-5 的冗余技术，一个 DS 组（由 5 台 DS 组
成）可提供的存储空间是该组中 DS 最小存储空间的 4 倍。文件系统中的每个文
件在每台 MS 中大约由 3 个文件表示。因此，保证 MS 有足够 inode 是非常重
要的。这些文件绝大多数都是单块（single block）文件，所以，除了分拨给
inode 的空间，它们占用的 MS 存储空间很小。在格式化 MS 使用的本地目录
时，这些因素都须要考虑。

为及早把资料写入存储，我们推荐使用类似 67108864（64 MB）的较小
`dirty_background_bytes` 内核 sysctl 设定。例如，可以在
`/etc/rc.d/rc.local` 加上以下一行来设置：

    echo 67108864 > /proc/sys/vm/dirty_background_bytes

这取代 `dirty_background_ratio` 的设定。你也应检视
`/etc/rc.d/rc.local` 里因版本而异的其他建议。

## 网络 ##

为了保证高性能，建议为 CPFS 服务器配置一个高性能的后端网络，最好是
InfiniBand（IB）网络或者万兆以太网。CPFS 系统本身对网络速度没有要求，因
此，不考虑性能，用 100Mbps 的网络构建一个 CPFS 系统来简单代替 NFS 是可
行的。

CPFS 程序使用 IP 网络协议。如果要使用 InfiniBand，则需要通过 IPoIB 实现
网络连接。为了获得最佳性能，IP 网络的 MTU 设定要足够高，使绝大多数网络
传输不被分段。如果网络允许，MTU 的值至少应为 34000。

# CPFS 安装 #

CPFS 的安装包括如下步骤：

  * 安装 CPFS 软件包。
  * 配置服务器：通过编辑 `/etc/sysconfig/cpfs-meta` 配置 MS， 和
    `/etc/sysconfig/cpfs-data` 配置 DS。
  * License 设置：把 license 文件拷贝到运行 MS 的服务器中，完成
    license 设置。
  * 设定身份验证密钥：在一台服务器中生成身份验证密钥，然后把它传送给所
    有运行 MS、DS 和 FS 的机器。
  * 配置 CPFS 客户端：通过设定 FC 机器上的 /etc/fstab 配置 CPFS 客户端。
  * 启动 CPFS：启动所有服务器，然后启动所有客户端。

这些步骤将在下面的章节中详细讨论。

## 安装 CPFS 软件包 ##

您可以下载 CPFS 软件包，也可以自行以源代码构建（参考 `build.md` 来创建
软件包）。对基于 Redhat 的系统（CentOS，RHEL，Fedora），可运行 `yum
localinstall <filename>` 安装（在 CentOS 7 请先执行 `yum install
epel-release`）。对基于 Debian 的系统，可运行 `dpkg -i <filename>` 然
后 `apt-get install -f`。包装包括以下内容。

  * `etc/init.d/cpfs-meta` 和 `etc/init.d/cpfs-data`: 兼容 LSB 的初始
    化脚本。
  * `etc/default/cpfs-meta` 和 `etc/default/cpfs-data`: 用于初始化脚本
    的配置文件。
  * `usr/sbin/mount.cpfs`: 用于挂载 CPFS 客户端的脚本。
  * `usr/local/sbin/cpfs_server` 和 `usr/local/sbin/cpfs_client`:
    实际文件系统服务器和客户端。
  * `usr/local/sbin/cpfs_keygen`: 产生共享的秘密的小程序。
  * `usr/local/sbin/cpfs_cli`: 监控服务器的命令行界面。
  * `usr/local/sbin/cpfs_configure_client`: 生成 `/etc/fstab` 来挂载
    CPFS 的一个小程序。
  * `usr/share/doc/cpfs`: 文档。

## 配置服务器 ##

编辑 `/etc/default/cpfs-meta` 和 `/etc/default/cpfs-data` 配置文件。多
数情况下，文件内容在所有运行 MS/DS 的服务器完全一致，因此，可以在一台
服务器上生成该文件，然后用 `scp` 命令或其他方法发送给其他服务器。下面
我们逐条说明该文件中的设置。在 `cpfs-meta` 和 `cpfs-data` 中：

    METADATA_SERVER="192.168.133.59:5000,192.168.133.60:5001"

指定 MS 使用的 IP 地址和端口号。我们称第一个为 MS1，第二个（如果有的话）
为 MS2。你须要确保所用的端口号不被防火墙阻挡。例如，对于 CentOS 6 的默
认防火墙，您可以在 `/etc/sysconfig/iptables` 中添加规则：

    -A INPUT -m state --state NEW -m tcp -p tcp --dport 5000 -j ACCEPT

然后运行 `service iptables reload`。 在 CentOS 7 或近期的 Fedora 中，
您可以在所有 MS 运行：

    firewall-cmd --zone=public --add-port=5000/tcp

在 `cpfs-meta` 中：

    METADATA_DIR=/var/lib/cpfs-meta

这是元数据服务器存储元数据的目录。该目录的权限必须是 0711。除了通过
CPFS 提供的方法之外不能用任何其它方法修改此目录，否则会导致数据不一致。

    MS_PERMS=0

CPFS 支援两种权限检查模式。默认情况下，CPFS 使用 FC-based 的模式，以
FUSE 的 `default_permissions` 选项提供权限检查，MS 不作检查。你也可选
用 MS-based 的模式，设定 `MS_PERMS` 为 1，删除 FC 上的
`default_permissions` 选项。这样，权限的检查便会转移到 MS 上。两者有以
下分别：

  * FC-based 的模式对 SUID 程序中附加组的处理较好。使用 MS-based 的权
    限模式，SUID 程序无法以原用户的附加用户组读写档案。他们会以程式档
    拥有者的附加用户组读写档案。
  * 如果扩充属性未被禁用，MS-based 的模式支援 POSIX 访问控制列表（ACL）。
    由于 FUSE 未能支援 ACL，在 FC-based 的模式下，ACL 的设定会被忽略。

请注意，您还应适当地设置客户端以选择权限模型。

    MS_EXTRA_ARGS=()

传递给元数据服务器的额外参数。 有效的参数可用 `cpfs_server --help` 找
到。

在 `cpfs-data` 中：

    DATA_DIR=/var/lib/cpfs

数据服务器存储文件数据的目录。该目录的权限必须是 0700。除了通过 CPFS 提
供的方法之外不能用任何其它方法修改此目录，否则会导致数据不一致。

    DS_HOST=192.168.133.20
    DS_PORT=5500

指定 DS 使用的 IP 地址和端口号。两个设置都有合理的默认值，如果默认值不
适合你的工作环境，可以自己设定。`DS_HOST` 默认值是发送数据包到第一个
MS 的本地 IP 地址，`DS_PORT` 的默认值是 6000。你须要确保所用的端口号不
被防火墙阻挡（可参考 MS 的指示）。如果你想所有 DS 使用同一个配置文件，
`DS_HOST` 设置应留空。

    DS_EXTRA_ARGS=()

传递给数据服务器的额外参数。 有效的参数可用 `cpfs_server --help` 找到。

## 设定身份验证密钥 ##

为了防止对 CPFS 集群未经授权的访问，MS、DS 和 FC 使用一个共享的密钥相
互认证。该密钥存放在一个密钥文件中，该文件从 `/etc/cpfs.key` 或
`CPFS_KEY_PATH` 环境变量（如在初始化脚本设定的话）指定的文件加载。你可
以用 `cpfs_keygen <path to cpfs.key>` 命令生成密钥。该文件必须设置为仅
root 可以读取，并分发到所有运行 CPFS的服务器和客户端的 `/etc/cpfs.key`。

## 配置客户端 ##

通过运行带有稍微复杂选项的 `mount` 命令来运行 FC。通常可以通过设置
`/etc/fstab` 实现机器启动后自动运行 FC。可以用
`cpfs_configure_client` 工具生成如下入口。

    $ sudo cpfs_configure_client
    Enter the meta server(s): (IP1:Port,IP2:Port) 192.168.0.1:5000
    Enter the path to mount point: /var/lib/cpfs
    Enter the path to the log file: /var/log/cpfs_client.log
    Use MS-based permission [n]?
    The following entry will be added to /etc/fstab:
    192.168.0.1:5000 /var/lib/cpfs cpfs log-path=/var/log/cpfs_client.log,\
    default_permissions,_netdev 0 0
    Continue? (y/n)

也可以通过如下命令手工挂载 FC：

    $ sudo mount -t cpfs <meta server ip:port> <mount> \
    > -o log-path=<log path>,default_permissions

CPFS 支援扩充属性（extended attributes）和访问控制列表（access control
list）。如果你不使用这功能，想避免性能开销，可以加入以下选项以禁用此功
能：

    disable-xattr=1

## 启动 CPFS ##

您可以利用初始化脚本启动 CPFS MS 或 DS：

    $ sudo service cpfs-meta start  # MS
    $ sudo service cpfs-data start  # DS

请注意，使用 init 脚本停止服务器（如 `service cpfs-server stop`）就像
使服务器意外停止并启动故障转移。 可用于暂时关闭服务器电源或进行其他操
作。 但是大多数时候，你应使用正常的关机（见下文）。

要将系统设置为在重新启动时自动启动 CPFS，请参阅伺服器的 init 系统。 例
如，对于 systemd 系统，您可以执行：

    $ sudo systemctl enable cpfs-meta  # For MS
    $ sudo systemctl enable cpfs-data  # For DS

运行如下命令启动 FC：

    $ sudo mount <mount point>

## 增加新的 DS 组 ##

创建之初，CPFS 只有一个 DS 组。可以用命令行管理客户端每次增加一个 DS 组。
例如，`config set MS1 num_ds_groups 2` 设定 DS 组的数量为 2。该配置生效
后，就可以添加新的 DS 到新生成的 DS 组。当 5 个 DS 被添加到该组后，该组
就准备就绪，可以接受文件数据存取。同样的 `config` 管理客户端命令也可以
用来移除 DS 组，注意，只有从来没有准备就绪的 DS 组才可以被移除。

在某些环境下，CPFS 日志输出可能较多，时间久了会变得过大。在这种情况下，
您可以使用 `logrotate` 定期旋转到一个新的日志文件并删除过旧的日志。例
如，元数据服务器的日志可以使用以下 `logrotate` 配置进行管理：

    /var/log/cpfs-meta.log {
        rotate 5
        weekly
        postrotate
            /bin/kill -HUP `cat /var/run/cpfs-meta.pid`
        endscript
    }

管理客户端的日志，配置中的命令可使用 `/usr/bin/pgrep -f
'/usr/local/sbin/cpfs_client.* <mount-point>'`。

# 管理 #

## 用多个 DS 组存放同一个文件 ##

通常，CPFS 中的文件存放在一个 DS 组中。这可以在文件产生时设定其父目录
的 `user.ndsg` 扩展属性来改变。例如，下面的命令设定 `<parent dir>` 产生
可最多横跨两个 DS 组的文件。这也导致 `<parent dir>` 目录下新产生的子目录
也具有相同的 `user.ndsg` 设置。

    $ setfattr -n user.ndsg -v 2 <parent dir>

## 命令行管理客户端 ##

CPFS 提供了一个命令行管理客户端 `cpfs_cli` ，用于 CPFS 的监控和管理。
支持如下命令。

  * `help`：显示支持的命令。
  * `status`：查询 CPFS 节点的状态。
  * `info`：查询 CPFS 储存空间使用率。
  * `config list`：列出设置项和它们的值。
  * `config set <target node> <config> <value>`：修改 target node 的设
    置项。指定 `<target node>` 的格式跟 `config list` 列出节点的格式相
    同，如 `MS1`，`DS 0-1` 等。可被修改的设置包括
    `log_severity`，`log_path` 和 `num_ds_groups`。
  * `system shutdown`：关闭 CPFS 系统。

用下列命名启动 `cpfs_cli`：

    $ sudo cpfs_cli --meta-server=<ip:port of MS1>[,ip:port of M2] [command]

在 MS 使用，毋须使用 `--meta-server` 参数。

使用两个 MS（HA 模式）的情况下，CPFS 通常要等两个 MS 都启动运行正常后才
会对外服务。如果只有一个正常运行，可以用 `cpfs_cli` 并指定
`--force-start=true` 选项在从 MS（slave MS）不正常的情况下启动 CPFS。

## 故障排除 ##

为了排除服务器端的错误，把 `/etc/default/cpfs-meta` 和
`/etc/default/cpfs-data` 中的 `LOG_LEVEL` 设置为 6，以便得到更详细的错
误消息。最大的日志级别是 7，但是级别 7 会产生大量的 debug 信息，不建议
在生产环境中使用。对于 FC，可以通过传送 `-o -d` 参数给 `mount` 来启动
FUSE 的调试模式：

    $ sudo mount -t cpfs ... -o log-path=<log path> -o -d

除此之外，你也可以发送 SIGUSR2 信号到 CPFS 服务器或 mount 客户端程序，
以在日志显示一些资讯，如正在等待处理的信息和最近收到的请求。

# 关闭 CPFS #

CPFS 系统可以通过 `cpfs_cli` 关闭。你也可以通过下列命令关闭 CPFS：

    $ sudo kill -SIGUSR1  <PID of active MS>

这会关闭包括所有 MS、DS 和 FC 在内的整个 CPFS 系统。

FC 可以通过 `umount` 命令断开与 CPFS 服务器的连接。在某些场景中（例如，
CPFS 发生多重错误，或是遇到 FUSE 或 CPFS 的 bug），可能会发生 `umount`
无法完成的情况。这时候，可以用 `fusectl` 文件系统在不关机的情况下中断
FC。 如果 `fusectl` 没有被挂载的话，首先要挂载 `fusectl` 文件系统：

    $ sudo mount -t fusectl fusectl /sys/fs/fuse/connections/

然后，可以在 `/sys/fs/fuse/connections` 下发现以数字命名的目录（例如，
`20`），里面有一个名为 `abort`的文件。往这个文件中写入任意内容都将中止
所有 CPFS 的操作：所有没有完成的和新产生的请求将立刻收到请求失败的回复。
这时就可以 `umount` CPFS 文件系统了。
