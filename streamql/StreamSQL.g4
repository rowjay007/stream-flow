grammar StreamSQL;

query
  : selectStmt EOF
  ;

selectStmt
  : SELECT projection FROM source (WHERE predicate)?
  ;

projection
  : STAR
  | IDENT (COMMA IDENT)*
  ;

source
  : IDENT
  ;

predicate
  : IDENT EQ STRING
  ;

SELECT: 'SELECT';
FROM: 'FROM';
WHERE: 'WHERE';
STAR: '*';
COMMA: ',';
EQ: '=';
IDENT: [a-zA-Z_][a-zA-Z0-9_]*;
STRING: '\'' (~['\r\n])* '\'';
WS: [ \t\r\n]+ -> skip;
