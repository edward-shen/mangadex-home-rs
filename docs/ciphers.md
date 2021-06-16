# Ciphers

This client relies on rustls, which only supports a subset of TLS ciphers.
Specifically, only TLS 1.2 ECDSA GCM ciphers as well as all TLS 1.3 ciphers are
supported. This means that clients that only support older, more insecure
ciphers may not be able to connect to this client.

In practice, this means this client's failure rate may be higher than expected.
This is okay, and within specifications.

## Why even bother?

Well, Australia has officially banned hentai... so I gotta make sure my mates
over there won't get in trouble if I'm connecting to them.