# CPFS System Protection #

CPFS is a distributed system relying on connections among servers to provide
the service. Proper protection is required to protect against users opening
fake connections as FC or acting as a fake MS / DS to intercept data.

After installation of CPFS, system administrator would generate a secret key
using the provided tool and store to a path only accesible by root.

Connections are initiated in four ways: FC to MS, FC to DS, MS2 to MS1 and
DS to MS. The following protocol is used to authenticate the connecting and
listening side:

 1. The connecting peer generates a random challenge CN and sends a connect
    FIM along with the CN to the server
 2. The server accepting connection generates a random challenge SN,
    computes and sends Hash(Encrypt(SN + CN)), SN to the connecting peer
 3. The connecting peer computes the expected hash value with the same
    secret key and SN received. If the hash value matches the one received,
    another hash value Hash(Encrypt(CN + SN)) is computed and sent to the peer.
    Otherwise, the connection is terminated
 4. The server computes the expected hash value with the same secret key
    and CN received. If the hash value matches the one received, connection
    registration is declared successful and the server will reply normally

This protocol utilizes random challenges to prevent against replay attack,
and performs mutual authentication, however this does not prevent MITM
evasdropping. To prevent evasdropping, the connection must be encryted but
this will incur overhead to the performance of CPFS. Therefore, channel
encryption is not considered.

The secret key and encryption algorithm should be strong enough so that
brute force attempt is not possible. On the other hand, the random challenge
should be unique and random enough so that the challenge generated is not
predictable and overlapping with challenges generated from other CPFS
components.
