/*
 * This is a port of the SQLite tokenize (http://www.sqlite.org/), with some 
 * features (such as EBCDIC support) removed.
 * 
 * Since there's very little original content here, I (Ami) disclaim copyright
 * for this file and include the orginal legal notice:
 *
 * 2001 September 15
 *
 * The author disclaims copyright to this source code.  In place of
 * a legal notice, here is a blessing:
 *
 *    May you do good and not evil.
 *    May you find forgiveness for yourself and forgive others.
 *    May you share freely, never taking more than you give.
 *
 ************************************************************************
 * A tokenizer for SQL
 *
 * This file contains Rust code that splits an SQL input string up into
 * individual tokens and sends those tokens one-by-one over to the
 * parser for analysis.
 */

/* Character classes for tokenizing
 *
 * In the sqlite3GetToken() function, a switch() on aiClass[c] is implemented
 * using a lookup table, whereas a switch() directly on c uses a binary search.
 * The lookup table is much faster.  To maximize speed, and to ensure that
 * a lookup table is used, all of the classes need to be small integers and
 * all of them need to be used within the switch.
 */
enum CharacterClass
{
	X,        /* The letter 'x', or start of BLOB literal */
	KYWD,     /* Alphabetics or '_'.  Usable in a keyword */
	ID,       /* unicode characters usable in IDs */
	DIGIT,    /* Digits */
	DOLLAR,   /* '$' */
	VARALPHA, /* '@', '#', ':'.  Alphabetic SQL variables */
	VARNUM,   /* '?'.  Numeric SQL variables */
	SPACE,    /* Space characters */
	QUOTE,    /* '"', '\'', or '`'.  String literals, quoted ids */
	QUOTE2,   /* '['.   [...] style quoted ids */
	PIPE,     /* '|'.   Bitwise OR or concatenate */
	MINUS,    /* '-'.  Minus or SQL-style comment */
	LT,       /* '<'.  Part of < or <= or <> */
	GT,       /* '>'.  Part of > or >= */
	EQ,       /* '='.  Part of = or == */
	BANG,     /* '!'.  Part of != */
	SLASH,    /* '/'.  / or c-style comment */
	LP,       /* '(' */
	RP,       /* ')' */
	SEMI,     /* ';' */
	PLUS,     /* '+' */
	STAR,     /* '*' */
	PERCENT , /* '%' */
	COMMA,    /* ',' */
	AND,      /* '&' */
	TILDA,    /* '~' */
	DOT ,     /* '.' */
	ILLEGAL , /* Illegal character */
}
static CLASS_LOOKUP: [CharacterClass; 128] = [
/*       x0       x1       x2       x3       x4       x5       x6       x7       x8       x9       xa       xb       xc       xd       xe       xf */
/* 0x */ ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, SPACE,   SPACE,   ILLEGAL, SPACE,   SPACE,   ILLEGAL, ILLEGAL,
/* 1x */ ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL, ILLEGAL,
/* 2x */ SPACE,   BANG,    QUOTE,   VARALPHA,DOLLAR,  PERCENT, AND,     QUOTE,   LP,      RP,      STAR,    PLUS,    COMMA,   MINUS,   DOT,     SLASH,
/* 3x */ DIGIT,   DIGIT,   DIGIT,   DIGIT,   DIGIT,   DIGIT,   DIGIT,   DIGIT,   DIGIT,   DIGIT,   VARALPHA,SEMI,    LT,      EQ,      GT,      VARNUM,
/* 4x */ VARALPHA,KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,
/* 5x */ KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    X,       KYWD,    KYWD,    QUOTE2,  ILLEGAL, ILLEGAL, ILLEGAL, KYWD,
/* 6x */ QUOTE,   KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,
/* 7x */ KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    KYWD,    X,       KYWD,    KYWD,    ILLEGAL, PIPE,    ILLEGAL, TILDA,   ILLEGAL,
];

/* The SQLite tokenizer generates a hash function to look up keywords.
 * That really seems like overkill (and would require porting the hash
 * generator as well), so we just use a simple table with binary search.
 *
 * This is presumably slower than the SQLite implementation, so if 
 * parser performance is critical for you, consider porting the original
 * SQLite technique over and submitting a patch.
 */
enum Token
{
	Abort,
	Action,
	Add,
	After,
	All,
	Alter,
	Analyze,
	And,
	As,
	Asc,
	Attach,
	Autoincr,
	Before,
	Begin,
	Between,
	By,
	Cascade,
	Case,
	Cast,
	Check,
	Collate,
	Column,
	Commit,
	Conflict,
	Constraint,
	Create,
	CTime,
	Database,
	Default,
	Deferrable,
	Deferred,
	Delete,
	Desc,
	Detach,
	Distinct,
	Drop,
	Each,
	Else,
	End,
	Escape,
	Except,
	Exclusive,
	Exists,
	Explain,
	Fail,
	For,
	Foreign,
	From,
	Group,
	Having,
	If,
	Ignore,
	Immediate,
	In,
	Index,
	Indexed,
	Initially,
	Inner,
	Insert,
	Instead,
	Intersect,
	Into,
	Is,
	IsNull,
	Join,
	Key,
	Like,
	Limit,
	Match,
	No,
	Not,
	NotNull,
	Null,
	Of,
	Offset,
	On,
	Or,
	Order,
	Plan,
	Pragma,
	Primary,
	Query,
	Raise,
	Recursive,
	References,
	Reindex,
	Release,
	Rename,
	Replace,
	Restrict,
	Rollback,
	Row,
	Savepoint,
	Select,
	Set,
	Table,
	Temp,
	Then,
	To,
	Transaction,
	Trigger,
	Union,
	Update,
	Using,
	Vacuum,
	Values,
	View,
	Virtual,
	With,
	Without,
	When,
	Where,
}

static KEYWORDS: &'static [(&'static str, Token)] = &[
	("ABORT", Abort),
	("ACTION", Action),
	("ADD", Add),
	("AFTER", After),
	("ALL", All),
	("ALTER", Alter),
	("ANALYZE", Analyze),
	("AND", And),
	("AS", As),
	("ASC", Asc),
	("ATTACH", Attach),
	("AUTOINCREMENT", Autoincr),
	("BEFORE", Before),
	("BEGIN", Begin),
	("BETWEEN", Between),
	("BY", By),
	("CASCADE", Cascade),
	("CASE", Case),
	("CAST", Cast),
	("CHECK", Check),
	("COLLATE", Collate),
	("COLUMN", Column),
	("COMMIT", Commit),
	("CONFLICT", Conflict),
	("CONSTRAINT", Constraint),
	("CREATE", Create),
	("CROSS", Join),
	("CURRENT_DATE", CTime),
	("CURRENT_TIME", CTime),
	("CURRENT_TIMESTAMP", CTime),
	("DATABASE", Database),
	("DEFAULT", Default),
	("DEFERRED", Deferred),
	("DEFERRABLE", Deferrable),
	("DELETE", Delete),
	("DESC", Desc),
	("DETACH", Detach),
	("DISTINCT", Distinct),
	("DROP", Drop),
	("END", End),
	("EACH", Each),
	("ELSE", Else),
	("ESCAPE", Escape),
	("EXCEPT", Except),
	("EXCLUSIVE", Exclusive),
	("EXISTS", Exists),
	("EXPLAIN", Explain),
	("FAIL", Fail),
	("FOR", For),
	("FOREIGN", Foreign),
	("FROM", From),
	("FULL", Join),
	("GLOB", Like),
	("GROUP", Group),
	("HAVING", Having),
	("IF", If),
	("IGNORE", Ignore),
	("IMMEDIATE", Immediate),
	("IN", In),
	("INDEX", Index),
	("INDEXED", Indexed),
	("INITIALLY", Initially),
	("INNER", Inner),
	("INSERT", Insert),
	("INSTEAD", Instead),
	("INTERSECT", Intersect),
	("INTO", Into),
	("IS", Is),
	("ISNULL", IsNull),
	("JOIN", Join),
	("KEY", Key),
	("LEFT", Join),
	("LIKE", Like),
	("LIMIT", Limit),
	("MATCH", Match),
	("NATURAL", Join),
	("NO", No),
	("NOT", Not),
	("NOTNULL", NotNull),
	("NULL", Null),
	("OF", Of),
	("OFFSET", Offset),
	("ON", On),
	("OR", Or),
	("ORDER", Order),
	("OUTER", Join),
	("PLAN", Plan),
	("PRAGMA", Pragma),
	("PRIMARY", Primary),
	("QUERY", Query),
	("RAISE", Raise),
	("RECURSIVE", Recursive),
	("REFERENCES", References),
	("REGEXP", Like),
	("REINDEX", Reindex),
	("RELEASE", Release),
	("RENAME", Rename),
	("REPLACE", Replace),
	("RESTRICT", Restrict),
	("RIGHT", Join),
	("ROLLBACK", Rollback),
	("ROW", Row),
	("SAVEPOINT", Savepoint),
	("SELECT", Select),
	("SET", Set),
	("TABLE", Table),
	("TEMP", Temp),
	("TEMPORARY", Temp),
	("THEN", Then),
	("TO", To),
	("TRANSACTIOIN", Transaction),
	("TRIGGER", Trigger),
	("UNION", Union),
	("UPDATE", Update),
	("USING", Using),
	("VACUUM", Vacuum),
	("VALUES", Values),
	("VIEW", View),
	("VIRTUAL", Virtual),
	("WITH", With),
	("WITHOUT", Without),
	("WHEN", When),
	("WHERE", Where),
];

impl CharacterClass
{
	fn from_char(c : char) -> Self
	{
		match c {
			0..127 => CLASS_LOOKUP[c],
			_ => Id
		}
	}
}

/*
 * If X is a character that can be used in an identifier then
 * IdChar(X) will be true.  Otherwise it is false.
 *
 * SQLite Ticket #1066.  the SQL standard does not allow '$' in the
 * middle of identifiers.  But many SQL implementations do. 
 * SQLite will allow '$' in identifiers for compatibility.
 * But the feature is undocumented.
 */

 */
fn id_char(c : char) -> bool
{
	match CharacterClass::from_char(c) {
		KYWD => true,
		ID => true,
		_ => false
	}
}

/*
** Return the length (in bytes) of the token that begins at z[0]. 
** Store the token type in *tokenType before returning.
*/
int sqlite3GetToken(const unsigned char *z, int *tokenType){
  int i, c;
  switch( aiClass[*z] ){  /* Switch on the character-class of the first byte
                          ** of the token. See the comment on the CC_ defines
                          ** above. */
    case CC_SPACE: {
      testcase( z[0]==' ' );
      testcase( z[0]=='\t' );
      testcase( z[0]=='\n' );
      testcase( z[0]=='\f' );
      testcase( z[0]=='\r' );
      for(i=1; sqlite3Isspace(z[i]); i++){}
      *tokenType = TK_SPACE;
      return i;
    }
    case CC_MINUS: {
      if( z[1]=='-' ){
        for(i=2; (c=z[i])!=0 && c!='\n'; i++){}
        *tokenType = TK_SPACE;   /* IMP: R-22934-25134 */
        return i;
      }
      *tokenType = TK_MINUS;
      return 1;
    }
    case CC_LP: {
      *tokenType = TK_LP;
      return 1;
    }
    case CC_RP: {
      *tokenType = TK_RP;
      return 1;
    }
    case CC_SEMI: {
      *tokenType = TK_SEMI;
      return 1;
    }
    case CC_PLUS: {
      *tokenType = TK_PLUS;
      return 1;
    }
    case CC_STAR: {
      *tokenType = TK_STAR;
      return 1;
    }
    case CC_SLASH: {
      if( z[1]!='*' || z[2]==0 ){
        *tokenType = TK_SLASH;
        return 1;
      }
      for(i=3, c=z[2]; (c!='*' || z[i]!='/') && (c=z[i])!=0; i++){}
      if( c ) i++;
      *tokenType = TK_SPACE;   /* IMP: R-22934-25134 */
      return i;
    }
    case CC_PERCENT: {
      *tokenType = TK_REM;
      return 1;
    }
    case CC_EQ: {
      *tokenType = TK_EQ;
      return 1 + (z[1]=='=');
    }
    case CC_LT: {
      if( (c=z[1])=='=' ){
        *tokenType = TK_LE;
        return 2;
      }else if( c=='>' ){
        *tokenType = TK_NE;
        return 2;
      }else if( c=='<' ){
        *tokenType = TK_LSHIFT;
        return 2;
      }else{
        *tokenType = TK_LT;
        return 1;
      }
    }
    case CC_GT: {
      if( (c=z[1])=='=' ){
        *tokenType = TK_GE;
        return 2;
      }else if( c=='>' ){
        *tokenType = TK_RSHIFT;
        return 2;
      }else{
        *tokenType = TK_GT;
        return 1;
      }
    }
    case CC_BANG: {
      if( z[1]!='=' ){
        *tokenType = TK_ILLEGAL;
        return 1;
      }else{
        *tokenType = TK_NE;
        return 2;
      }
    }
    case CC_PIPE: {
      if( z[1]!='|' ){
        *tokenType = TK_BITOR;
        return 1;
      }else{
        *tokenType = TK_CONCAT;
        return 2;
      }
    }
    case CC_COMMA: {
      *tokenType = TK_COMMA;
      return 1;
    }
    case CC_AND: {
      *tokenType = TK_BITAND;
      return 1;
    }
    case CC_TILDA: {
      *tokenType = TK_BITNOT;
      return 1;
    }
    case CC_QUOTE: {
      int delim = z[0];
      testcase( delim=='`' );
      testcase( delim=='\'' );
      testcase( delim=='"' );
      for(i=1; (c=z[i])!=0; i++){
        if( c==delim ){
          if( z[i+1]==delim ){
            i++;
          }else{
            break;
          }
        }
      }
      if( c=='\'' ){
        *tokenType = TK_STRING;
        return i+1;
      }else if( c!=0 ){
        *tokenType = TK_ID;
        return i+1;
      }else{
        *tokenType = TK_ILLEGAL;
        return i;
      }
    }
    case CC_DOT: {
#ifndef SQLITE_OMIT_FLOATING_POINT
      if( !sqlite3Isdigit(z[1]) )
#endif
      {
        *tokenType = TK_DOT;
        return 1;
      }
      /* If the next character is a digit, this is a floating point
      ** number that begins with ".".  Fall thru into the next case */
    }
    case CC_DIGIT: {
      testcase( z[0]=='0' );  testcase( z[0]=='1' );  testcase( z[0]=='2' );
      testcase( z[0]=='3' );  testcase( z[0]=='4' );  testcase( z[0]=='5' );
      testcase( z[0]=='6' );  testcase( z[0]=='7' );  testcase( z[0]=='8' );
      testcase( z[0]=='9' );
      *tokenType = TK_INTEGER;
#ifndef SQLITE_OMIT_HEX_INTEGER
      if( z[0]=='0' && (z[1]=='x' || z[1]=='X') && sqlite3Isxdigit(z[2]) ){
        for(i=3; sqlite3Isxdigit(z[i]); i++){}
        return i;
      }
#endif
      for(i=0; sqlite3Isdigit(z[i]); i++){}
#ifndef SQLITE_OMIT_FLOATING_POINT
      if( z[i]=='.' ){
        i++;
        while( sqlite3Isdigit(z[i]) ){ i++; }
        *tokenType = TK_FLOAT;
      }
      if( (z[i]=='e' || z[i]=='E') &&
           ( sqlite3Isdigit(z[i+1]) 
            || ((z[i+1]=='+' || z[i+1]=='-') && sqlite3Isdigit(z[i+2]))
           )
      ){
        i += 2;
        while( sqlite3Isdigit(z[i]) ){ i++; }
        *tokenType = TK_FLOAT;
      }
#endif
      while( IdChar(z[i]) ){
        *tokenType = TK_ILLEGAL;
        i++;
      }
      return i;
    }
    case CC_QUOTE2: {
      for(i=1, c=z[0]; c!=']' && (c=z[i])!=0; i++){}
      *tokenType = c==']' ? TK_ID : TK_ILLEGAL;
      return i;
    }
    case CC_VARNUM: {
      *tokenType = TK_VARIABLE;
      for(i=1; sqlite3Isdigit(z[i]); i++){}
      return i;
    }
    case CC_DOLLAR:
    case CC_VARALPHA: {
      int n = 0;
      testcase( z[0]=='$' );  testcase( z[0]=='@' );
      testcase( z[0]==':' );  testcase( z[0]=='#' );
      *tokenType = TK_VARIABLE;
      for(i=1; (c=z[i])!=0; i++){
        if( IdChar(c) ){
          n++;
#ifndef SQLITE_OMIT_TCL_VARIABLE
        }else if( c=='(' && n>0 ){
          do{
            i++;
          }while( (c=z[i])!=0 && !sqlite3Isspace(c) && c!=')' );
          if( c==')' ){
            i++;
          }else{
            *tokenType = TK_ILLEGAL;
          }
          break;
        }else if( c==':' && z[i+1]==':' ){
          i++;
#endif
        }else{
          break;
        }
      }
      if( n==0 ) *tokenType = TK_ILLEGAL;
      return i;
    }
    case CC_KYWD: {
      for(i=1; aiClass[z[i]]<=CC_KYWD; i++){}
      if( IdChar(z[i]) ){
        /* This token started out using characters that can appear in keywords,
        ** but z[i] is a character not allowed within keywords, so this must
        ** be an identifier instead */
        i++;
        break;
      }
      *tokenType = TK_ID;
      return keywordCode((char*)z, i, tokenType);
    }
    case CC_X: {
#ifndef SQLITE_OMIT_BLOB_LITERAL
      testcase( z[0]=='x' ); testcase( z[0]=='X' );
      if( z[1]=='\'' ){
        *tokenType = TK_BLOB;
        for(i=2; sqlite3Isxdigit(z[i]); i++){}
        if( z[i]!='\'' || i%2 ){
          *tokenType = TK_ILLEGAL;
          while( z[i] && z[i]!='\'' ){ i++; }
        }
        if( z[i] ) i++;
        return i;
      }
#endif
      /* If it is not a BLOB literal, then it must be an ID, since no
      ** SQL keywords start with the letter 'x'.  Fall through */
    }
    case CC_ID: {
      i = 1;
      break;
    }
    default: {
      *tokenType = TK_ILLEGAL;
      return 1;
    }
  }
  while( IdChar(z[i]) ){ i++; }
  *tokenType = TK_ID;
  return i;
}

/*
** Run the parser on the given SQL string.  The parser structure is
** passed in.  An SQLITE_ status code is returned.  If an error occurs
** then an and attempt is made to write an error message into 
** memory obtained from sqlite3_malloc() and to make *pzErrMsg point to that
** error message.
*/
int sqlite3RunParser(Parse *pParse, const char *zSql, char **pzErrMsg){
  int nErr = 0;                   /* Number of errors encountered */
  int i;                          /* Loop counter */
  void *pEngine;                  /* The LEMON-generated LALR(1) parser */
  int tokenType;                  /* type of the next token */
  int lastTokenParsed = -1;       /* type of the previous token */
  sqlite3 *db = pParse->db;       /* The database connection */
  int mxSqlLen;                   /* Max length of an SQL string */

  assert( zSql!=0 );
  mxSqlLen = db->aLimit[SQLITE_LIMIT_SQL_LENGTH];
  if( db->nVdbeActive==0 ){
    db->u1.isInterrupted = 0;
  }
  pParse->rc = SQLITE_OK;
  pParse->zTail = zSql;
  i = 0;
  assert( pzErrMsg!=0 );
  /* sqlite3ParserTrace(stdout, "parser: "); */
  pEngine = sqlite3ParserAlloc(sqlite3Malloc);
  if( pEngine==0 ){
    sqlite3OomFault(db);
    return SQLITE_NOMEM_BKPT;
  }
  assert( pParse->pNewTable==0 );
  assert( pParse->pNewTrigger==0 );
  assert( pParse->nVar==0 );
  assert( pParse->nzVar==0 );
  assert( pParse->azVar==0 );
  while( 1 ){
    assert( i>=0 );
    if( zSql[i]!=0 ){
      pParse->sLastToken.z = &zSql[i];
      pParse->sLastToken.n = sqlite3GetToken((u8*)&zSql[i],&tokenType);
      i += pParse->sLastToken.n;
      if( i>mxSqlLen ){
        pParse->rc = SQLITE_TOOBIG;
        break;
      }
    }else{
      /* Upon reaching the end of input, call the parser two more times
      ** with tokens TK_SEMI and 0, in that order. */
      if( lastTokenParsed==TK_SEMI ){
        tokenType = 0;
      }else if( lastTokenParsed==0 ){
        break;
      }else{
        tokenType = TK_SEMI;
      }
    }
    if( tokenType>=TK_SPACE ){
      assert( tokenType==TK_SPACE || tokenType==TK_ILLEGAL );
      if( db->u1.isInterrupted ){
        pParse->rc = SQLITE_INTERRUPT;
        break;
      }
      if( tokenType==TK_ILLEGAL ){
        sqlite3ErrorMsg(pParse, "unrecognized token: \"%T\"",
                        &pParse->sLastToken);
        break;
      }
    }else{
      sqlite3Parser(pEngine, tokenType, pParse->sLastToken, pParse);
      lastTokenParsed = tokenType;
      if( pParse->rc!=SQLITE_OK || db->mallocFailed ) break;
    }
  }
  assert( nErr==0 );
  pParse->zTail = &zSql[i];
#ifdef YYTRACKMAXSTACKDEPTH
  sqlite3_mutex_enter(sqlite3MallocMutex());
  sqlite3StatusHighwater(SQLITE_STATUS_PARSER_STACK,
      sqlite3ParserStackPeak(pEngine)
  );
  sqlite3_mutex_leave(sqlite3MallocMutex());
#endif /* YYDEBUG */
  sqlite3ParserFree(pEngine, sqlite3_free);
  if( db->mallocFailed ){
    pParse->rc = SQLITE_NOMEM_BKPT;
  }
  if( pParse->rc!=SQLITE_OK && pParse->rc!=SQLITE_DONE && pParse->zErrMsg==0 ){
    pParse->zErrMsg = sqlite3MPrintf(db, "%s", sqlite3ErrStr(pParse->rc));
  }
  assert( pzErrMsg!=0 );
  if( pParse->zErrMsg ){
    *pzErrMsg = pParse->zErrMsg;
    sqlite3_log(pParse->rc, "%s", *pzErrMsg);
    pParse->zErrMsg = 0;
    nErr++;
  }
  if( pParse->pVdbe && pParse->nErr>0 && pParse->nested==0 ){
    sqlite3VdbeDelete(pParse->pVdbe);
    pParse->pVdbe = 0;
  }
#ifndef SQLITE_OMIT_SHARED_CACHE
  if( pParse->nested==0 ){
    sqlite3DbFree(db, pParse->aTableLock);
    pParse->aTableLock = 0;
    pParse->nTableLock = 0;
  }
#endif
#ifndef SQLITE_OMIT_VIRTUALTABLE
  sqlite3_free(pParse->apVtabLock);
#endif

  if( !IN_DECLARE_VTAB ){
    /* If the pParse->declareVtab flag is set, do not delete any table 
    ** structure built up in pParse->pNewTable. The calling code (see vtab.c)
    ** will take responsibility for freeing the Table structure.
    */
    sqlite3DeleteTable(db, pParse->pNewTable);
  }

  if( pParse->pWithToFree ) sqlite3WithDelete(db, pParse->pWithToFree);
  sqlite3DeleteTrigger(db, pParse->pNewTrigger);
  for(i=pParse->nzVar-1; i>=0; i--) sqlite3DbFree(db, pParse->azVar[i]);
  sqlite3DbFree(db, pParse->azVar);
  while( pParse->pAinc ){
    AutoincInfo *p = pParse->pAinc;
    pParse->pAinc = p->pNext;
    sqlite3DbFree(db, p);
  }
  while( pParse->pZombieTab ){
    Table *p = pParse->pZombieTab;
    pParse->pZombieTab = p->pNextZombie;
    sqlite3DeleteTable(db, p);
  }
  assert( nErr==0 || pParse->rc!=SQLITE_OK );
  return nErr;
}
