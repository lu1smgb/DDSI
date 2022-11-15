CREATE TABLE Stock
(
    Cproducto NUMBER CONSTRAINT Cproducto_no_nulo NOT NULL
        CONSTRAINT Cproducto_clave_primaria PRIMARY KEY,
    Cantidad NUMBER CONSTRAINT Cantidad_no_nulo NOT NULL
);

CREATE TABLE Pedido
(
    Cpedido NUMBER CONSTRAINT Cpedido_no_nulo NOT NULL
        CONSTRAINT Cpedido_clave_primaria PRIMARY KEY,
    Ccliente NUMBER CONSTRAINT Ccliente_no_nulo NOT NULL,
    FechaPedido DATE CONSTRAINT FechaPedido_no_nulo NOT NULL
);

CREATE TABLE DetallePedido
(
    Cpedido CONSTRAINT Cpedido_clave_externa_Pedido
        REFERENCES Pedido (Cpedido),
    Cproducto CONSTRAINT Cproducto_clave_externa_Stock
        REFERENCES Stock (Cproducto),
    Cantidad_pedido NUMBER CONSTRAINT Cantidad_pedido_no_nulo NOT NULL,
    CONSTRAINT clave_primaria PRIMARY KEY (Cpedido, Cproducto)
);