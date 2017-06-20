cat <<EOF
Summary: ClusterTech Parallel Filesystem server, client, tools and documentation
Name: cpfs-os
Version: ${VERSION}
Release: 1
License: AGPL 3
Group: System Environment/Daemons
Vendor: ClusterTech Ltd
Source0: %{name}.tar.gz

Requires: fuse, redhat-lsb-core


%description
${DESCRIPTION}


%prep
%setup -c %{name}-%{version} -q


%build
%cmake -DCMAKE_INSTALL_PREFIX:PATH=/usr/local -DCMAKE_BUILD_TYPE=Release .
make %{?_smp_mflags}


%install
make install DESTDIR=%{buildroot}


%post
systemctl 2>&1 | grep -q -- '-\.mount' && systemctl daemon-reload || true


%postun
systemctl 2>&1 | grep -q -- '-\.mount' && systemctl daemon-reload || true


%files
%defattr(-,root,root,-)
%dir "/usr/local"
"/usr/local/sbin"
%dir "/usr/local/share"
"/usr/local/share/doc"
%dir "/etc/default"
%config "/sbin/mount.cpfs"
%config "/etc/init.d/cpfs-meta"
%config "/etc/init.d/cpfs-data"
%config(noreplace) "/etc/default/cpfs-meta"
%config(noreplace) "/etc/default/cpfs-data"
EOF
