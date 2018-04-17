# CPFS System Protection #

CPFS is a distributed system relying on connections among servers to provide
the service. Proper protection is required to protect against users opening
fake connections as FCs or even as fake MSs / DSs to intercept data.

After installation of CPFS, system administrator would generate a
secret key using the provided tool, and ensure that it is accesible
only by root.

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

This protocol utilizes random challenges to prevent replay attack, and
performs mutual authentication, however this does not prevent MITM
evasdropping. To prevent evasdropping, the connection must be encryted
but this slows down the system. Therefore, channel encryption is not
considered.
