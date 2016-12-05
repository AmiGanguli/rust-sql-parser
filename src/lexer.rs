/*
 * Generously hacked LambdaDB tokenizer, with keywords added from SQLite.
 */

#[derive(Clone, Debug, PartialEq)]
pub struct Token
{
	token_type: TokenType,
	line: usize,
	column: usize,
	value: String,
}

impl Token
{
	pub fn new(tt: TokenType, line: usize, column: usize, value: String) -> Self
	{
		Self {
			token_type: tt,
			line: line,
			column: column,
			value: value
		}
	}
	// A convenience function to help write test cases more concisely.
	//
	pub fn vec( tuples: &[(TokenType, usize, usize, &str)] ) -> Vec<Self>
	{
		let mut ret = Vec::new();
		for tuple in tuples {
			ret.push(Self::new(tuple.0, tuple.1, tuple.2, tuple.3.to_string()));
		}
		ret
	}
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TokenType
{
	// Keywords
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
	Left,
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

	// Non-letter tokens
	Equal,
	NotEqual,
	LessThan,
	LessThanOrEqual,
	GreaterThan,
	GreaterThanOrEqual,

	Plus, Minus,

	LeftParen, RightParen,
	LeftBracket, RightBracket,
	Dot, Comma, Semicolon,

	Ampersand, Pipe, ForwardSlash,

	/// ||, the concatenate operator
	DoublePipe,

	/// *, the wildcard for SELECT
	Asterisk,

	/// ?, the prepared statement placeholder
	PreparedStatementPlaceholder,
	
	// Tokens with values
	Ident,
	Number,
	StringLiteral,
}

enum LexerState {
	NoState,
	Word,
	Backtick,
	Apostrophe { escaping: bool },
	Number { decimal: bool },
	/// Disambiguate an operator sequence.
	OperatorDisambiguate { first: char },
	LineComment,
	BlockComment { was_prev_char_asterisk: bool }
}

pub struct Lexer {
	pub tokens: Vec<Token>,
	state: LexerState,
	string_buffer: String,
	start_line_number: usize,
	start_column_number: usize,
	line_number: usize,
	column_number: usize
}

impl Lexer {
	pub fn new() -> Lexer
	{
		Lexer {
			tokens: Vec::new(),
			state: LexerState::NoState,
			string_buffer: String::new(),
			start_column_number: 0,
			start_line_number: 1,
			column_number: 0,
			line_number: 1,
		}
	}

	fn push_token(&mut self, tt: TokenType, word: String)
	{
		self.tokens.push(Token {
			line: self.start_line_number, 
			column: self.start_column_number, 
			value: word, 
			token_type: tt 
		});
	}
	
	fn character_to_token(&self, c: char) -> Option<Token>
	{
		use self::TokenType::*;

		let tt: TokenType;
	
		match c {
			'=' => tt = Equal,
			'<' => tt = LessThan,
			'>' => tt = GreaterThan,
			'+' => tt = Plus,
			'-' => tt = Minus,
			'(' => tt = LeftParen,
			')' => tt = RightParen,
			'[' => tt = LeftBracket,
			']' => tt = RightBracket,
			'.' => tt = Dot,
			',' => tt = Comma,
			';' => tt = Semicolon,
			'&' => tt = Ampersand,
			'|' => tt = Pipe,
			'*' => tt = Asterisk,
			'/' => tt = ForwardSlash,
			'?' => tt = PreparedStatementPlaceholder,
			_ => return None
		};
		Some(Token{ 
			line: self.start_line_number, 
			column: self.start_column_number, 
			value: c.to_string(), 
			token_type: tt 
		})
	}

	fn push_character(&mut self, c: char)
	{
		let token = self.character_to_token(c).unwrap();
		self.tokens.push(token);
	}

	fn push_word(&mut self)
	{
		use self::TokenType::*;
		let word = self.move_string_buffer();
		let uppercase: String = word.chars().flat_map( |c| c.to_uppercase() ).collect();
		let tt: TokenType;
		
		match uppercase.as_ref() {
			"ABORT" => tt = Abort,
			"ACTION" => tt = Action,
			"ADD" => tt = Add,
			"AFTER" => tt = After,
			"ALL" => tt = All,
			"ALTER" => tt = Alter,
			"ANALYZE" => tt = Analyze,
			"AND" => tt = And,
			"AS" => tt = As,
			"ASC" => tt = Asc,
			"ATTACH" => tt = Attach,
			"AUTOINCREMENT" => tt = Autoincr,
			"BEFORE" => tt = Before,
			"BEGIN" => tt = Begin,
			"BETWEEN" => tt = Between,
			"BY" => tt = By,
			"CASCADE" => tt = Cascade,
			"CASE" => tt = Case,
			"CAST" => tt = Cast,
			"CHECK" => tt = Check,
			"COLLATE" => tt = Collate,
			"COLUMN" => tt = Column,
			"COMMIT" => tt = Commit,
			"CONFLICT" => tt = Conflict,
			"CONSTRAINT" => tt = Constraint,
			"CREATE" => tt = Create,
			"CROSS" => tt = Join,
			"CURRENT_DATE" => tt = CTime,
			"CURRENT_TIME" => tt = CTime,
			"CURRENT_TIMESTAMP" => tt = CTime,
			"DATABASE" => tt = Database,
			"DEFAULT" => tt = Default,
			"DEFERRED" => tt = Deferred,
			"DEFERRABLE" => tt = Deferrable,
			"DELETE" => tt = Delete,
			"DESC" => tt = Desc,
			"DETACH" => tt = Detach,
			"DISTINCT" => tt = Distinct,
			"DROP" => tt = Drop,
			"END" => tt = End,
			"EACH" => tt = Each,
			"ELSE" => tt = Else,
			"ESCAPE" => tt = Escape,
			"EXCEPT" => tt = Except,
			"EXCLUSIVE" => tt = Exclusive,
			"EXISTS" => tt = Exists,
			"EXPLAIN" => tt = Explain,
			"FAIL" => tt = Fail,
			"FOR" => tt = For,
			"FOREIGN" => tt = Foreign,
			"FROM" => tt = From,
			"FULL" => tt = Join,
			"GLOB" => tt = Like,
			"GROUP" => tt = Group,
			"HAVING" => tt = Having,
			"IF" => tt = If,
			"IGNORE" => tt = Ignore,
			"IMMEDIATE" => tt = Immediate,
			"IN" => tt = In,
			"INDEX" => tt = Index,
			"INDEXED" => tt = Indexed,
			"INITIALLY" => tt = Initially,
			"INNER" => tt = Inner,
			"INSERT" => tt = Insert,
			"INSTEAD" => tt = Instead,
			"INTERSECT" => tt = Intersect,
			"INTO" => tt = Into,
			"IS" => tt = Is,
			"ISNULL" => tt = IsNull,
			"JOIN" => tt = Join,
			"KEY" => tt = Key,
			"LEFT" => tt = Left,
			"LIKE" => tt = Like,
			"LIMIT" => tt = Limit,
			"MATCH" => tt = Match,
			"NATURAL" => tt = Join,
			"NO" => tt = No,
			"NOT" => tt = Not,
			"NOTNULL" => tt = NotNull,
			"NULL" => tt = Null,
			"OF" => tt = Of,
			"OFFSET" => tt = Offset,
			"ON" => tt = On,
			"OR" => tt = Or,
			"ORDER" => tt = Order,
			"OUTER" => tt = Join,
			"PLAN" => tt = Plan,
			"PRAGMA" => tt = Pragma,
			"PRIMARY" => tt = Primary,
			"QUERY" => tt = Query,
			"RAISE" => tt = Raise,
			"RECURSIVE" => tt = Recursive,
			"REFERENCES" => tt = References,
			"REGEXP" => tt = Like,
			"REINDEX" => tt = Reindex,
			"RELEASE" => tt = Release,
			"RENAME" => tt = Rename,
			"REPLACE" => tt = Replace,
			"RESTRICT" => tt = Restrict,
			"RIGHT" => tt = Join,
			"ROLLBACK" => tt = Rollback,
			"ROW" => tt = Row,
			"SAVEPOINT" => tt = Savepoint,
			"SELECT" => tt = Select,
			"SET" => tt = Set,
			"TABLE" => tt = Table,
			"TEMP" => tt = Temp,
			"TEMPORARY" => tt = Temp,
			"THEN" => tt = Then,
			"TO" => tt = To,
			"TRANSACTIOIN" => tt = Transaction,
			"TRIGGER" => tt = Trigger,
			"UNION" => tt = Union,
			"UPDATE" => tt = Update,
			"USING" => tt = Using,
			"VACUUM" => tt = Vacuum,
			"VALUES" => tt = Values,
			"VIEW" => tt = View,
			"VIRTUAL" => tt = Virtual,
			"WITH" => tt = With,
			"WITHOUT" => tt = Without,
			"WHEN" => tt = When,
			"WHERE" => tt = Where,
			_ => tt = Ident
		};
		self.tokens.push(Token{ 
			line: self.start_line_number, 
			column: self.start_column_number, 
			value: word, 
			token_type: tt 
		});
	}

	pub fn is_no_state(&self) -> bool
	{
		match self.state {
			LexerState::NoState => true,
			_ => false
		}
	}

	fn no_state(&mut self, c: char) -> Result<LexerState, char>
	{
		self.start_line_number = self.line_number;
		self.start_column_number = self.column_number;
		match c {
			'a'...'z' | 'A'...'Z' | '_' => {
				self.string_buffer.push(c);
				Ok(LexerState::Word)
			},
			'`' => {
				Ok(LexerState::Backtick)
			}
			'\'' => {
				// string literal
				Ok(LexerState::Apostrophe { escaping: false })
			},
			'0'...'9' => {
				self.string_buffer.push(c);
				Ok(LexerState::Number { decimal: false })
			},
			' ' | '\t' | '\n' => {
				// whitespace
				Ok(LexerState::NoState)
			},
			c => {
				use self::TokenType::*;
				match self.character_to_token(c) {
					Some(token) => {
						match token.token_type {
							LessThan | GreaterThan | Minus | Pipe | ForwardSlash => {
								Ok(LexerState::OperatorDisambiguate { first: c })
							}
							_ => {
								self.tokens.push(token);
								Ok(LexerState::NoState)
							}
						}
					},
					None => {
						// unknown character
						Err(c)
					}
				}
			}
		}
	}

	fn move_string_buffer(&mut self) -> String
	{
		use std::mem;
		mem::replace(&mut self.string_buffer, String::new())
	}

	pub fn feed_character(&mut self, c: Option<char>)
	{
		match c {
			None => {},
			Some('\n') => {
				self.line_number = self.line_number + 1;
				self.column_number = 0
			},
			_ => self.column_number = self.column_number + 1
		}
		self.state = match self.state {
			LexerState::NoState => {
				match c {
					Some(c) => self.no_state(c).unwrap(),
					None => LexerState::NoState
				}
			},
			LexerState::Word => {
				match c {
					Some(c) => match c {
						'a'...'z' | 'A'...'Z' | '_' | '0'...'9' => {
							self.string_buffer.push(c);
							LexerState::Word
						}
						c => {
							self.push_word();
							self.no_state(c).unwrap()
						}
					},
					None => {
						self.push_word();
						LexerState::NoState
					}
				}
			},
			LexerState::Backtick => {
				use self::TokenType::*;
				match c {
					Some('`') => {
						let buffer = self.move_string_buffer();
						self.tokens.push(Token{
							line:self.start_line_number, 
							column:self.start_column_number,
							value: buffer,
							token_type: Ident
						});
						LexerState::NoState
					},
					Some(c) => {
						self.string_buffer.push(c);
						LexerState::Backtick
					},
					None => {
						// error: backtick did not finish
						unimplemented!()
					}
				}
			},
			LexerState::Apostrophe { escaping } => {
				if let Some(c) = c {
					use self::TokenType::*;
					match (escaping, c) {
						(false, '\'') => {
							// unescaped apostrophe
							let buffer = self.move_string_buffer();
							self.tokens.push(Token {
								line: self.start_line_number,
								column: self.start_column_number,
								value: buffer,
								token_type: StringLiteral
							});
							LexerState::NoState
						},
						(false, '\\') => {
							// unescaped backslash
							LexerState::Apostrophe { escaping: true }
						},
						(true, _) | _ => {
							self.string_buffer.push(c);
							LexerState::Apostrophe { escaping: false }
						}
					}
				} else {
					// error: apostrophe did not finish
					unimplemented!()
				}
			},
			LexerState::Number { decimal } => {
				if let Some(c) = c {
					match c {
						'0'...'9' => {
							self.string_buffer.push(c);
							LexerState::Number { decimal: decimal }
						},
						'.' if !decimal => {
							// Add a decimal point. None has been added yet.
							self.string_buffer.push(c);
							LexerState::Number { decimal: true }
						},
						c => {
							use self::TokenType::*;
							let buffer = self.move_string_buffer();
							self.tokens.push(Token {
								line: self.start_line_number,
								column: self.start_column_number,
								value: buffer,
								token_type: Number
							});
							self.no_state(c).unwrap()
						}
					}
				} else {
					use self::TokenType::*;
					let buffer = self.move_string_buffer();
					self.tokens.push(Token {
						line: self.start_line_number,
						column: self.start_column_number,
						value: buffer,
						token_type: Number
					});
					LexerState::NoState
				}
			},
			LexerState::OperatorDisambiguate { first } => {
				use self::TokenType::*;

				if let Some(c) = c {
					match (first, c) {
						('<', '>') => {
							self.push_token(NotEqual, "<>".to_string());
							LexerState::NoState
						},
						('<', '=') => {
							self.push_token(LessThanOrEqual, "<=".to_string());
							LexerState::NoState
						},
						('>', '=') => {
							self.push_token(GreaterThanOrEqual, ">=".to_string());
							LexerState::NoState
						},
						('|', '|') => {
							self.push_token(DoublePipe, "||".to_string());
							LexerState::NoState
						},
						('-', '-') => {
							LexerState::LineComment
						},
						('/', '*') => {
							LexerState::BlockComment { was_prev_char_asterisk: false }
						},
						_ => {
							self.push_character(first);
							self.no_state(c).unwrap()
						}
					}
				} else {
					self.push_character(first);
					LexerState::NoState
				}
			},
			LexerState::LineComment => {
				match c {
					Some('\n') => LexerState::NoState,
					_ => LexerState::LineComment
				}
			},
			LexerState::BlockComment { was_prev_char_asterisk } => {
				if was_prev_char_asterisk && c == Some('/') {
					LexerState::NoState
				} else {
					LexerState::BlockComment { was_prev_char_asterisk: c == Some('*') }
				}
			}
		};
	}

	pub fn feed_characters<I>(&mut self, iter: I)
	where I: Iterator<Item=char>
	{
		for c in iter {
			self.feed_character(Some(c));
		}
	}
}

pub fn parse(sql: &str) -> Vec<Token>
{
	let mut lexer = Lexer::new();

	lexer.feed_characters(sql.chars());
	lexer.feed_character(None);

	lexer.tokens
}

pub trait Tokenizer
{
	fn tokenize(&self) -> Vec<Token>;
}

impl Tokenizer for String
{
	fn tokenize(&self) -> Vec<Token>
	{
		parse(self)
	}
} 

#[cfg(test)]
mod test {
	use super::parse;
	use super::Token;
	use super::Tokenizer;

	#[test]
	fn test_sql_lexer_dontconfuseidentswithkeywords()
	{
		use super::TokenType::*;
		// Not: AS, Ident("df")
		assert_eq!(parse("asdf"), Token::vec(&[(Ident, 1, 1, "asdf")]));
		assert_eq!("asdf".to_string().tokenize(), Token::vec(&[(Ident, 1, 1, "asdf")]));
	}

	#[test]
	fn test_sql_lexer_escape()
	{
		use super::TokenType::*;
		// Escaped apostrophe
		assert_eq!(parse(r"'\''"), Token::vec(&[(StringLiteral, 1, 1, "'")]));
		assert_eq!(r"'\''".to_string().tokenize(), Token::vec(&[(StringLiteral, 1, 1, "'")]));
	}

	#[test]
	fn test_sql_lexer_numbers()
	{
		use super::TokenType::*;

		assert_eq!(parse("12345"), Token::vec(&[(Number, 1, 1, "12345")]));
		assert_eq!("12345".to_string().tokenize(), Token::vec(&[(Number, 1, 1, "12345")]));
		assert_eq!(parse("0.25"), Token::vec(&[(Number, 1, 1, "0.25")]));
		assert_eq!("0.25".to_string().tokenize(), Token::vec(&[(Number, 1, 1, "0.25")]));
		assert_eq!(parse("0.25 + -0.25"), Token::vec(&[(Number, 1, 1, "0.25"), (Plus, 1, 6, "+"), (Minus, 1, 8, "-"), (Number, 1, 9, "0.25")]));
		assert_eq!("-0.25 + 0.25".to_string().tokenize(), Token::vec(&[(Minus, 1, 1, "-"),(Number, 1, 2, "0.25"), (Plus, 1, 7, "+"), (Number, 1, 9, "0.25")]));
		assert_eq!("- 0.25 - -0.25".to_string().tokenize(), Token::vec(&[(Minus, 1, 1, "-"),(Number, 1, 3, "0.25"), (Minus, 1, 8, "-"), (Minus, 1, 10, "-"), (Number, 1, 11, "0.25")]));
		assert_eq!("- 0.25 --0.25".to_string().tokenize(), Token::vec(&[(Minus, 1, 1, "-"),(Number, 1, 3, "0.25")]));
		assert_eq!("0.25 -0.25".to_string().tokenize(), Token::vec(&[(Number, 1, 1, "0.25"), (Minus, 1, 6, "-"), (Number, 1, 7, "0.25")]));
	}

	#[test]
	fn test_sql_lexer_query1()
	{
		use super::TokenType::*;

		assert_eq!(
			parse(" SeLECT a,    b as alias1 , c alias2, d ` alias three ` \nfRoM table1 WHERE a='Hello World'; "),
			Token::vec(&[
				(Select, 1, 2, "SeLECT"), (Ident, 1, 9, "a"), (Comma, 1, 10, ","), (Ident, 1, 15, "b"), (As, 1, 17, "as"), (Ident, 1, 20, "alias1"), (Comma, 1, 27, ","),
				(Ident, 1, 29, "c"), (Ident, 1, 31, "alias2"), (Comma, 1, 37, ","), (Ident, 1, 39, "d"), (Ident, 1, 41, " alias three "),
				(From, 2, 1, "fRoM"), (Ident, 2, 6, "table1"),
				(Where, 2, 13, "WHERE"), (Ident, 2, 19, "a"), (Equal, 2, 20, "="), (StringLiteral, 2, 21, "Hello World"), (Semicolon, 2, 34, ";")
			])
		);
	}

	#[test]
	fn test_sql_lexer_query2() {
		use super::TokenType::*;

		let query = r"
		-- Get employee count from each department
		SELECT d.id departmentId, count(e.id) employeeCount
		FROM department d
		LEFT JOIN employee e ON e.departmentId = d.id
		GROUP BY departmentId;
		";

		assert_eq!(parse(query), Token::vec(&[
			(Select, 3, 3, "SELECT"), (Ident, 3, 10, "d"), (Dot, 3, 11, "."), (Ident, 3, 12, "id"), (Ident, 3, 15, "departmentId"),
			(Comma, 3, 27, ","), (Ident, 3, 29, "count"), (LeftParen, 3, 34, "("), (Ident, 3, 35, "e"), (Dot, 3, 36, "."), (Ident, 3, 37, "id"), (RightParen, 3, 39, ")"), (Ident, 3, 41, "employeeCount"),
			(From, 4, 3, "FROM"), (Ident, 4, 8, "department"), (Ident, 4, 19, "d"),
			(Left, 5, 3, "LEFT"), (Join, 5, 8, "JOIN"), (Ident, 5, 13, "employee"), (Ident, 5, 22, "e"), (On, 5, 24, "ON"), (Ident, 5, 27, "e"), 
			(Dot, 5, 28, "."), (Ident, 5, 29, "departmentId"), (Equal, 5, 42, "="), (Ident, 5, 44, "d"), (Dot, 5, 45, "."), (Ident, 5, 46, "id"),
			(Group, 6, 3, "GROUP"), (By, 6, 9, "BY"), (Ident, 6, 12, "departmentId"), (Semicolon, 6, 24, ";")
		]));
	}

	#[test]
	fn test_sql_lexer_operators() {
		use super::TokenType::*;

		assert_eq!(parse("> = >=< =><"),
			Token::vec(&[
				(GreaterThan, 1, 1, ">"), (Equal, 1, 3, "="), (GreaterThanOrEqual, 1, 5, ">="), (LessThan, 1, 7, "<"), (Equal, 1, 9, "="), (GreaterThan, 1, 10, ">"), (LessThan, 1, 11, "<")
			])
		);

		assert_eq!(parse(" ><>> >< >"),
			Token::vec(&[
				(GreaterThan, 1, 2, ">"), (NotEqual, 1, 3, "<>"), (GreaterThan, 1, 5, ">"), (GreaterThan, 1, 7, ">"), (LessThan, 1, 8, "<"), (GreaterThan, 1, 10, ">")
			])
		);
	}
	#[test]
	fn test_sql_lexer_blockcomment() {
		use super::TokenType::*;

		assert_eq!(parse("hello/*/a/**/,/*there, */world"), Token::vec(&[
			(Ident, 1, 1, "hello"), (Comma, 1, 14, ","), (Ident, 1, 26, "world")
		]));
		assert_eq!(parse("/ */"), Token::vec(&[(ForwardSlash, 1, 1, "/"), (Asterisk, 1, 3, "*"), (ForwardSlash, 1, 4, "/")]));
		assert_eq!(parse("/**/"), Token::vec(&[]));
		assert_eq!(parse("a/* test\ntest** /\nb*/c"), Token::vec(&[(Ident, 1, 1, "a"), (Ident, 3, 4, "c")]));
	}
}

