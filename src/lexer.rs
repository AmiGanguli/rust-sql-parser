/*
 * Generously hacked LambdaDB tokenizer, with keywords added from SQLite.
 */

use tokens::*;

macro_rules! tokens {
	( $( $x:expr ),* ) => {
		{
			let mut _temp_vec = Vec::new();
			$(
				_temp_vec.push(Token::new($x.0, $x.1, $x.2, $x.3.to_string()));
			)*
			_temp_vec
		}
	};
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
		use tokens::TokenType::*;

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
		use tokens::TokenType::*;
		let word = self.move_string_buffer();
		let uppercase: String = word.chars().flat_map( |c| c.to_uppercase() ).collect();
		let tt: TokenType;
		
		tt = match uppercase.as_ref() {
			"ABORT" => Abort,
			"ACTION" => Action,
			"ADD" => Add,
			"AFTER" => After,
			"ALL" => All,
			"ALTER" => Alter,
			"ANALYZE" => Analyze,
			"AND" => And,
			"AS" => As,
			"ASC" => Asc,
			"ATTACH" => Attach,
			"AUTOINCREMENT" => Autoincr,
			"BEFORE" => Before,
			"BEGIN" => Begin,
			"BETWEEN" => Between,
			"BY" => By,
			"CASCADE" => Cascade,
			"CASE" => Case,
			"CAST" => Cast,
			"CHECK" => Check,
			"COLLATE" => Collate,
			"COLUMN" => Column,
			"COMMIT" => Commit,
			"CONFLICT" => Conflict,
			"CONSTRAINT" => Constraint,
			"CREATE" => Create,
			"CROSS" => Join,
			"CURRENT_DATE" => CTime,
			"CURRENT_TIME" => CTime,
			"CURRENT_TIMESTAMP" => CTime,
			"DATABASE" => Database,
			"DEFAULT" => Default,
			"DEFERRED" => Deferred,
			"DEFERRABLE" => Deferrable,
			"DELETE" => Delete,
			"DESC" => Desc,
			"DETACH" => Detach,
			"DISTINCT" => Distinct,
			"DROP" => Drop,
			"END" => End,
			"EACH" => Each,
			"ELSE" => Else,
			"ESCAPE" => Escape,
			"EXCEPT" => Except,
			"EXCLUSIVE" => Exclusive,
			"EXISTS" => Exists,
			"EXPLAIN" => Explain,
			"FAIL" => Fail,
			"FOR" => For,
			"FOREIGN" => Foreign,
			"FROM" => From,
			"FULL" => Join,
			"GLOB" => Like,
			"GROUP" => Group,
			"HAVING" => Having,
			"IF" => If,
			"IGNORE" => Ignore,
			"IMMEDIATE" => Immediate,
			"IN" => In,
			"INDEX" => Index,
			"INDEXED" => Indexed,
			"INITIALLY" => Initially,
			"INNER" => Inner,
			"INSERT" => Insert,
			"INSTEAD" => Instead,
			"INTERSECT" => Intersect,
			"INTO" => Into,
			"IS" => Is,
			"ISNULL" => IsNull,
			"JOIN" => Join,
			"KEY" => Key,
			"LEFT" => Left,
			"LIKE" => Like,
			"LIMIT" => Limit,
			"MATCH" => Match,
			"NATURAL" => Join,
			"NO" => No,
			"NOT" => Not,
			"NOTNULL" => NotNull,
			"NULL" => Null,
			"OF" => Of,
			"OFFSET" => Offset,
			"ON" => On,
			"OR" => Or,
			"ORDER" => Order,
			"OUTER" => Join,
			"PLAN" => Plan,
			"PRAGMA" => Pragma,
			"PRIMARY" => Primary,
			"QUERY" => Query,
			"RAISE" => Raise,
			"RECURSIVE" => Recursive,
			"REFERENCES" => References,
			"REGEXP" => Like,
			"REINDEX" => Reindex,
			"RELEASE" => Release,
			"RENAME" => Rename,
			"REPLACE" => Replace,
			"RESTRICT" => Restrict,
			"RIGHT" => Join,
			"ROLLBACK" => Rollback,
			"ROW" => Row,
			"SAVEPOINT" => Savepoint,
			"SELECT" => Select,
			"SET" => Set,
			"TABLE" => Table,
			"TEMP" => Temp,
			"TEMPORARY" => Temp,
			"THEN" => Then,
			"TO" => To,
			"TRANSACTIOIN" => Transaction,
			"TRIGGER" => Trigger,
			"UNION" => Union,
			"UPDATE" => Update,
			"USING" => Using,
			"VACUUM" => Vacuum,
			"VALUES" => Values,
			"VIEW" => View,
			"VIRTUAL" => Virtual,
			"WITH" => With,
			"WITHOUT" => Without,
			"WHEN" => When,
			"WHERE" => Where,
			_ => Ident
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
				use tokens::TokenType::*;
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
				use tokens::TokenType::*;
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
					use tokens::TokenType::*;
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
							use tokens::TokenType::*;
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
					use tokens::TokenType::*;
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
				use tokens::TokenType::*;

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
	use tokens::*;
	use tokens::TokenType::*;
	use super::Tokenizer;

	#[test]
	fn test_sql_lexer_dontconfuseidentswithkeywords()
	{
		// Not: AS, Ident("df")
		assert_eq!(parse("asdf"), tokens![(Ident, 1, 1, "asdf")]);
		assert_eq!("asdf".to_string().tokenize(), tokens![(Ident, 1, 1, "asdf")]);
	}

	#[test]
	fn test_sql_lexer_escape()
	{
		// Escaped apostrophe
		assert_eq!(parse(r"'\''"), tokens![(StringLiteral, 1, 1, "'")]);
		assert_eq!(r"'\''".to_string().tokenize(), tokens![(StringLiteral, 1, 1, "'")]);
	}

	#[test]
	fn test_sql_lexer_numbers()
	{
		assert_eq!(parse("12345"), tokens![(Number, 1, 1, "12345")]);
		assert_eq!("12345".to_string().tokenize(), tokens![(Number, 1, 1, "12345")]);
		assert_eq!(parse("0.25"), tokens![(Number, 1, 1, "0.25")]);
		assert_eq!("0.25".to_string().tokenize(), tokens![(Number, 1, 1, "0.25")]);
		assert_eq!(parse("0.25 + -0.25"), tokens![(Number, 1, 1, "0.25"), (Plus, 1, 6, "+"), (Minus, 1, 8, "-"), (Number, 1, 9, "0.25")]);
		assert_eq!("-0.25 + 0.25".to_string().tokenize(), tokens![(Minus, 1, 1, "-"),(Number, 1, 2, "0.25"), (Plus, 1, 7, "+"), (Number, 1, 9, "0.25")]);
		assert_eq!("- 0.25 - -0.25".to_string().tokenize(), tokens![(Minus, 1, 1, "-"),(Number, 1, 3, "0.25"), (Minus, 1, 8, "-"), (Minus, 1, 10, "-"), (Number, 1, 11, "0.25")]);
		assert_eq!("- 0.25 --0.25".to_string().tokenize(), tokens![(Minus, 1, 1, "-"),(Number, 1, 3, "0.25")]);
		assert_eq!("0.25 -0.25".to_string().tokenize(), tokens![(Number, 1, 1, "0.25"), (Minus, 1, 6, "-"), (Number, 1, 7, "0.25")]);
	}

	#[test]
	fn test_sql_lexer_query1()
	{
		assert_eq!(
			parse(" SeLECT a,    b as alias1 , c alias2, d ` alias three ` \nfRoM table1 WHERE a='Hello World'; "),
			tokens![
				(Select, 1, 2, "SeLECT"), (Ident, 1, 9, "a"), (Comma, 1, 10, ","), (Ident, 1, 15, "b"), (As, 1, 17, "as"), (Ident, 1, 20, "alias1"), (Comma, 1, 27, ","),
				(Ident, 1, 29, "c"), (Ident, 1, 31, "alias2"), (Comma, 1, 37, ","), (Ident, 1, 39, "d"), (Ident, 1, 41, " alias three "),
				(From, 2, 1, "fRoM"), (Ident, 2, 6, "table1"),
				(Where, 2, 13, "WHERE"), (Ident, 2, 19, "a"), (Equal, 2, 20, "="), (StringLiteral, 2, 21, "Hello World"), (Semicolon, 2, 34, ";")
			]
		);
	}

	#[test]
	fn test_sql_lexer_query2() {
		let query = r"
		-- Get employee count from each department
		SELECT d.id departmentId, count(e.id) employeeCount
		FROM department d
		LEFT JOIN employee e ON e.departmentId = d.id
		GROUP BY departmentId;
		";

		assert_eq!(parse(query), tokens![
			(Select, 3, 3, "SELECT"), (Ident, 3, 10, "d"), (Dot, 3, 11, "."), (Ident, 3, 12, "id"), (Ident, 3, 15, "departmentId"),
			(Comma, 3, 27, ","), (Ident, 3, 29, "count"), (LeftParen, 3, 34, "("), (Ident, 3, 35, "e"), (Dot, 3, 36, "."), (Ident, 3, 37, "id"), (RightParen, 3, 39, ")"), (Ident, 3, 41, "employeeCount"),
			(From, 4, 3, "FROM"), (Ident, 4, 8, "department"), (Ident, 4, 19, "d"),
			(Left, 5, 3, "LEFT"), (Join, 5, 8, "JOIN"), (Ident, 5, 13, "employee"), (Ident, 5, 22, "e"), (On, 5, 24, "ON"), (Ident, 5, 27, "e"), 
			(Dot, 5, 28, "."), (Ident, 5, 29, "departmentId"), (Equal, 5, 42, "="), (Ident, 5, 44, "d"), (Dot, 5, 45, "."), (Ident, 5, 46, "id"),
			(Group, 6, 3, "GROUP"), (By, 6, 9, "BY"), (Ident, 6, 12, "departmentId"), (Semicolon, 6, 24, ";")
		]);
	}

	#[test]
	fn test_sql_lexer_operators() {
		assert_eq!(parse("> = >=< =><"),
			tokens![
				(GreaterThan, 1, 1, ">"), (Equal, 1, 3, "="), (GreaterThanOrEqual, 1, 5, ">="), (LessThan, 1, 7, "<"), (Equal, 1, 9, "="), (GreaterThan, 1, 10, ">"), (LessThan, 1, 11, "<")
			]
		);

		assert_eq!(parse(" ><>> >< >"),
			tokens![
				(GreaterThan, 1, 2, ">"), (NotEqual, 1, 3, "<>"), (GreaterThan, 1, 5, ">"), (GreaterThan, 1, 7, ">"), (LessThan, 1, 8, "<"), (GreaterThan, 1, 10, ">")
			]
		);
	}
	#[test]
	fn test_sql_lexer_blockcomment() {
		assert_eq!(parse("hello/*/a/**/,/*there, */world"), tokens![
			(Ident, 1, 1, "hello"), (Comma, 1, 14, ","), (Ident, 1, 26, "world")
		]);
		assert_eq!(parse("/ */"), tokens![(ForwardSlash, 1, 1, "/"), (Asterisk, 1, 3, "*"), (ForwardSlash, 1, 4, "/")]);
		assert_eq!(parse("/**/"), tokens![]);
		assert_eq!(parse("a/* test\ntest** /\nb*/c"), tokens![(Ident, 1, 1, "a"), (Ident, 3, 4, "c")]);
	}
}
