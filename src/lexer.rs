/*
 * Based on the LambdaDB tokenizer, with keywords added from SQLite.
 */

#[derive(Clone, Debug, PartialEq)]
pub enum Token
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
	Ident(String),
	Number(String),
	StringLiteral(String),
}

fn character_to_token(c: char) -> Option<Token>
{
	use self::Token::*;

	Some(match c {
		'=' => Equal,
		'<' => LessThan,
		'>' => GreaterThan,
		'+' => Plus,
		'-' => Minus,
		'(' => LeftParen,
		')' => RightParen,
		'[' => LeftBracket,
		']' => RightBracket,
		'.' => Dot,
		',' => Comma,
		';' => Semicolon,
		'&' => Ampersand,
		'|' => Pipe,
		'*' => Asterisk,
		'/' => ForwardSlash,
		'?' => PreparedStatementPlaceholder,
		_ => return None
	})
}

fn word_to_token(word: String) -> Token
{
	use self::Token::*;
	let uppercase: String =word.chars().flat_map( |c| c.to_uppercase() ).collect();
	match uppercase.as_ref() {
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
		"JOIN" =>Join,
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
		_ => Ident(word)
	}
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
	string_buffer: String
}

impl Lexer {
	pub fn new() -> Lexer
	{
		Lexer {
			tokens: Vec::new(),
			state: LexerState::NoState,
			string_buffer: String::new()
		}
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
				use self::Token::*;

				match character_to_token(c) {
					Some(LessThan) | Some(GreaterThan) | Some(Minus) | Some(Pipe) | Some(ForwardSlash) => {
						Ok(LexerState::OperatorDisambiguate { first: c })
					},
					Some(token) => {
						self.tokens.push(token);
						Ok(LexerState::NoState)
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
							let buffer = self.move_string_buffer();
							self.tokens.push(word_to_token(buffer));
							self.no_state(c).unwrap()
						}
					},
					None => {
						let buffer = self.move_string_buffer();
						self.tokens.push(word_to_token(buffer));
						LexerState::NoState
					}
				}
			},
			LexerState::Backtick => {
				match c {
					Some('`') => {
						let buffer = self.move_string_buffer();
						self.tokens.push(Token::Ident(buffer));
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
					match (escaping, c) {
						(false, '\'') => {
							// unescaped apostrophe
							let buffer = self.move_string_buffer();
							self.tokens.push(Token::StringLiteral(buffer));
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
							let buffer = self.move_string_buffer();
							self.tokens.push(Token::Number(buffer));
							self.no_state(c).unwrap()
						}
					}
				} else {
					let buffer = self.move_string_buffer();
					self.tokens.push(Token::Number(buffer));
					LexerState::NoState
				}
			},
			LexerState::OperatorDisambiguate { first } => {
				use self::Token::*;

				if let Some(c) = c {
					match (first, c) {
						('<', '>') => {
							self.tokens.push(NotEqual);
							LexerState::NoState
						},
						('<', '=') => {
							self.tokens.push(LessThanOrEqual);
							LexerState::NoState
						},
						('>', '=') => {
							self.tokens.push(GreaterThanOrEqual);
							LexerState::NoState
						},
						('|', '|') => {
							self.tokens.push(DoublePipe);
							LexerState::NoState
						},
						('-', '-') => {
							LexerState::LineComment
						},
						('/', '*') => {
							LexerState::BlockComment { was_prev_char_asterisk: false }
						},
						_ => {
							self.tokens.push(character_to_token(first).unwrap());
							self.no_state(c).unwrap()
						}
					}
				} else {
					self.tokens.push(character_to_token(first).unwrap());
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
	use super::Tokenizer;

	fn id(value: &str) -> super::Token
	{
		super::Token::Ident(value.to_string())
	}

	fn number(value: &str) -> super::Token
	{
		super::Token::Number(value.to_string())
	}

	#[test]
	fn test_sql_lexer_dontconfuseidentswithkeywords()
	{
		use super::Token::*;
		// Not: AS, Ident("df")
		assert_eq!(parse("asdf"), vec![Ident("asdf".to_string())]);
		assert_eq!("asdf".to_string().tokenize(), vec![Ident("asdf".to_string())]);
	}

	#[test]
	fn test_sql_lexer_escape()
	{
		use super::Token::*;
		// Escaped apostrophe
		assert_eq!(parse(r"'\''"), vec![StringLiteral("'".to_string())]);
		assert_eq!(r"'\''".to_string().tokenize(), vec![StringLiteral("'".to_string())]);
	}

	#[test]
	fn test_sql_lexer_numbers()
	{
		use super::Token::*;

		assert_eq!(parse("12345"), vec![number("12345")]);
		assert_eq!("12345".to_string().tokenize(), vec![number("12345")]);
		assert_eq!(parse("0.25"), vec![number("0.25")]);
		assert_eq!("0.25".to_string().tokenize(), vec![number("0.25")]);
		assert_eq!(parse("0.25 + -0.25"), vec![number("0.25"), Plus, Minus, number("0.25")]);
		assert_eq!("0.25 + -0.25".to_string().tokenize(), vec![number("0.25"), Plus, Minus, number("0.25")]);
		assert_eq!(parse("-0.25 + 0.25"), vec![Minus, number("0.25"), Plus, number("0.25")]);
		assert_eq!("-0.25 + 0.25".to_string().tokenize(), vec![Minus, number("0.25"), Plus, number("0.25")]);
		assert_eq!(parse("- 0.25 - -0.25"), vec![Minus, number("0.25"), Minus, Minus, number("0.25")]);
		assert_eq!("- 0.25 - -0.25".to_string().tokenize(), vec![Minus, number("0.25"), Minus, Minus, number("0.25")]);
		assert_eq!(parse("- 0.25 --0.25"), vec![Minus, number("0.25")]);
		assert_eq!("- 0.25 --0.25".to_string().tokenize(), vec![Minus, number("0.25")]);
		assert_eq!(parse("0.25 -0.25"), vec![number("0.25"), Minus, number("0.25")]);
		assert_eq!("0.25 -0.25".to_string().tokenize(), vec![number("0.25"), Minus, number("0.25")]);
	}

	#[test]
	fn test_sql_lexer_query1()
	{
		use super::Token::*;

		assert_eq!(parse(" SeLECT a,    b as alias1 , c alias2, d ` alias three ` fRoM table1 WHERE a='Hello World'; "),
			vec![
				Select, id("a"), Comma, id("b"), As, id("alias1"), Comma,
				id("c"), id("alias2"), Comma, id("d"), id(" alias three "),
				From, id("table1"),
				Where, id("a"), Equal, StringLiteral("Hello World".to_string()), Semicolon
			]
		);
	}
	
	#[test]
	fn test_sql_lexer_query2() {
		use super::Token::*;

		let query = r"
		-- Get employee count from each department
		SELECT d.id departmentId, count(e.id) employeeCount
		FROM department d
		LEFT JOIN employee e ON e.departmentId = d.id
		GROUP BY departmentId;
		";

		assert_eq!(parse(query), vec![
			Select, id("d"), Dot, id("id"), id("departmentId"), Comma, id("count"), LeftParen, id("e"), Dot, id("id"), RightParen, id("employeeCount"),
			From, id("department"), id("d"),
			Left, Join, id("employee"), id("e"), On, id("e"), Dot, id("departmentId"), Equal, id("d"), Dot, id("id"),
			Group, By, id("departmentId"), Semicolon
		]);
	}
	
	#[test]
	fn test_sql_lexer_operators() {
		use super::Token::*;

		assert_eq!(parse("> = >=< =><"),
			vec![
				GreaterThan, Equal, GreaterThanOrEqual, LessThan, Equal, GreaterThan, LessThan
			]
		);

		assert_eq!(parse(" ><>> >< >"),
			vec![
				GreaterThan, NotEqual, GreaterThan, GreaterThan, LessThan, GreaterThan
			]
		);
	}
	
	#[test]
	fn test_sql_lexer_blockcomment() {
		use super::Token::*;

		assert_eq!(parse("hello/*/a/**/,/*there, */world"), vec![
			id("hello"), Comma, id("world")
		]);
		assert_eq!(parse("/ */"), vec![ForwardSlash, Asterisk, ForwardSlash]);
		assert_eq!(parse("/**/"), vec![]);
		assert_eq!(parse("a/* test\ntest** /\nb*/c"), vec![id("a"), id("c")]);
	}
}

