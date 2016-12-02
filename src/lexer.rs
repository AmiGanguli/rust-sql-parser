/*
 * Based on the LambdaDB tokenizer, with keywords added from SQLite.
 */

#[derive(Clone, Debug, PartialEq)]
pub enum Token
{
	// Keywords
	Abort(usize,usize),
	Action(usize,usize),
	Add(usize,usize),
	After(usize,usize),
	All(usize,usize),
	Alter(usize,usize),
	Analyze(usize,usize),
	And(usize,usize),
	As(usize,usize),
	Asc(usize,usize),
	Attach(usize,usize),
	Autoincr(usize,usize),
	Before(usize,usize),
	Begin(usize,usize),
	Between(usize,usize),
	By(usize,usize),
	Cascade(usize,usize),
	Case(usize,usize),
	Cast(usize,usize),
	Check(usize,usize),
	Collate(usize,usize),
	Column(usize,usize),
	Commit(usize,usize),
	Conflict(usize,usize),
	Constraint(usize,usize),
	Create(usize,usize),
	CTime(usize,usize),
	Database(usize,usize),
	Default(usize,usize),
	Deferrable(usize,usize),
	Deferred(usize,usize),
	Delete(usize,usize),
	Desc(usize,usize),
	Detach(usize,usize),
	Distinct(usize,usize),
	Drop(usize,usize),
	Each(usize,usize),
	Else(usize,usize),
	End(usize,usize),
	Escape(usize,usize),
	Except(usize,usize),
	Exclusive(usize,usize),
	Exists(usize,usize),
	Explain(usize,usize),
	Fail(usize,usize),
	For(usize,usize),
	Foreign(usize,usize),
	From(usize,usize),
	Group(usize,usize),
	Having(usize,usize),
	If(usize,usize),
	Ignore(usize,usize),
	Immediate(usize,usize),
	In(usize,usize),
	Index(usize,usize),
	Indexed(usize,usize),
	Initially(usize,usize),
	Inner(usize,usize),
	Insert(usize,usize),
	Instead(usize,usize),
	Intersect(usize,usize),
	Into(usize,usize),
	Is(usize,usize),
	IsNull(usize,usize),
	Join(usize,usize),
	Key(usize,usize),
	Left(usize,usize),
	Like(usize,usize),
	Limit(usize,usize),
	Match(usize,usize),
	No(usize,usize),
	Not(usize,usize),
	NotNull(usize,usize),
	Null(usize,usize),
	Of(usize,usize),
	Offset(usize,usize),
	On(usize,usize),
	Or(usize,usize),
	Order(usize,usize),
	Plan(usize,usize),
	Pragma(usize,usize),
	Primary(usize,usize),
	Query(usize,usize),
	Raise(usize,usize),
	Recursive(usize,usize),
	References(usize,usize),
	Reindex(usize,usize),
	Release(usize,usize),
	Rename(usize,usize),
	Replace(usize,usize),
	Restrict(usize,usize),
	Rollback(usize,usize),
	Row(usize,usize),
	Savepoint(usize,usize),
	Select(usize,usize),
	Set(usize,usize),
	Table(usize,usize),
	Temp(usize,usize),
	Then(usize,usize),
	To(usize,usize),
	Transaction(usize,usize),
	Trigger(usize,usize),
	Union(usize,usize),
	Update(usize,usize),
	Using(usize,usize),
	Vacuum(usize,usize),
	Values(usize,usize),
	View(usize,usize),
	Virtual(usize,usize),
	With(usize,usize),
	Without(usize,usize),
	When(usize,usize),
	Where(usize,usize),

	// Non-letter tokens
	Equal(usize,usize),
	NotEqual(usize,usize),
	LessThan(usize,usize),
	LessThanOrEqual(usize,usize),
	GreaterThan(usize,usize),
	GreaterThanOrEqual(usize,usize),

	Plus(usize,usize), Minus(usize,usize),

	LeftParen(usize,usize), RightParen(usize,usize),
	LeftBracket(usize,usize), RightBracket(usize,usize),
	Dot(usize,usize), Comma(usize,usize), Semicolon(usize,usize),

	Ampersand(usize,usize), Pipe(usize,usize), ForwardSlash(usize,usize),

	/// ||, the concatenate operator
	DoublePipe(usize,usize),

	/// *, the wildcard for SELECT
	Asterisk(usize,usize),

	/// ?, the prepared statement placeholder
	PreparedStatementPlaceholder(usize,usize),
	
	// Tokens with values
	Ident(String,usize,usize),
	Number(String,usize,usize),
	StringLiteral(String,usize,usize),
}

fn character_to_token(c: char, line_number: usize, column_number: usize) -> Option<Token>
{
	use self::Token::*;

	Some(match c {
		'=' => Equal(line_number, column_number),
		'<' => LessThan(line_number, column_number),
		'>' => GreaterThan(line_number, column_number),
		'+' => Plus(line_number, column_number),
		'-' => Minus(line_number, column_number),
		'(' => LeftParen(line_number, column_number),
		')' => RightParen(line_number, column_number),
		'[' => LeftBracket(line_number, column_number),
		']' => RightBracket(line_number, column_number),
		'.' => Dot(line_number, column_number),
		',' => Comma(line_number, column_number),
		';' => Semicolon(line_number, column_number),
		'&' => Ampersand(line_number, column_number),
		'|' => Pipe(line_number, column_number),
		'*' => Asterisk(line_number, column_number),
		'/' => ForwardSlash(line_number, column_number),
		'?' => PreparedStatementPlaceholder(line_number, column_number),
		_ => return None
	})
}

fn word_to_token(word: String, line_number: usize, column_number: usize) -> Token
{
	use self::Token::*;
	let uppercase: String =word.chars().flat_map( |c| c.to_uppercase() ).collect();
	match uppercase.as_ref() {
		"ABORT" => Abort(line_number, column_number),
		"ACTION" => Action(line_number, column_number),
		"ADD" => Add(line_number, column_number),
		"AFTER" => After(line_number, column_number),
		"ALL" => All(line_number, column_number),
		"ALTER" => Alter(line_number, column_number),
		"ANALYZE" => Analyze(line_number, column_number),
		"AND" => And(line_number, column_number),
		"AS" => As(line_number, column_number),
		"ASC" => Asc(line_number, column_number),
		"ATTACH" => Attach(line_number, column_number),
		"AUTOINCREMENT" => Autoincr(line_number, column_number),
		"BEFORE" => Before(line_number, column_number),
		"BEGIN" => Begin(line_number, column_number),
		"BETWEEN" => Between(line_number, column_number),
		"BY" => By(line_number, column_number),
		"CASCADE" => Cascade(line_number, column_number),
		"CASE" => Case(line_number, column_number),
		"CAST" => Cast(line_number, column_number),
		"CHECK" => Check(line_number, column_number),
		"COLLATE" => Collate(line_number, column_number),
		"COLUMN" => Column(line_number, column_number),
		"COMMIT" => Commit(line_number, column_number),
		"CONFLICT" => Conflict(line_number, column_number),
		"CONSTRAINT" => Constraint(line_number, column_number),
		"CREATE" => Create(line_number, column_number),
		"CROSS" => Join(line_number, column_number),
		"CURRENT_DATE" => CTime(line_number, column_number),
		"CURRENT_TIME" => CTime(line_number, column_number),
		"CURRENT_TIMESTAMP" => CTime(line_number, column_number),
		"DATABASE" => Database(line_number, column_number),
		"DEFAULT" => Default(line_number, column_number),
		"DEFERRED" => Deferred(line_number, column_number),
		"DEFERRABLE" => Deferrable(line_number, column_number),
		"DELETE" => Delete(line_number, column_number),
		"DESC" => Desc(line_number, column_number),
		"DETACH" => Detach(line_number, column_number),
		"DISTINCT" => Distinct(line_number, column_number),
		"DROP" => Drop(line_number, column_number),
		"END" => End(line_number, column_number),
		"EACH" => Each(line_number, column_number),
		"ELSE" => Else(line_number, column_number),
		"ESCAPE" => Escape(line_number, column_number),
		"EXCEPT" => Except(line_number, column_number),
		"EXCLUSIVE" => Exclusive(line_number, column_number),
		"EXISTS" => Exists(line_number, column_number),
		"EXPLAIN" => Explain(line_number, column_number),
		"FAIL" => Fail(line_number, column_number),
		"FOR" => For(line_number, column_number),
		"FOREIGN" => Foreign(line_number, column_number),
		"FROM" => From(line_number, column_number),
		"FULL" => Join(line_number, column_number),
		"GLOB" => Like(line_number, column_number),
		"GROUP" => Group(line_number, column_number),
		"HAVING" => Having(line_number, column_number),
		"IF" => If(line_number, column_number),
		"IGNORE" => Ignore(line_number, column_number),
		"IMMEDIATE" => Immediate(line_number, column_number),
		"IN" => In(line_number, column_number),
		"INDEX" => Index(line_number, column_number),
		"INDEXED" => Indexed(line_number, column_number),
		"INITIALLY" => Initially(line_number, column_number),
		"INNER" => Inner(line_number, column_number),
		"INSERT" => Insert(line_number, column_number),
		"INSTEAD" => Instead(line_number, column_number),
		"INTERSECT" => Intersect(line_number, column_number),
		"INTO" => Into(line_number, column_number),
		"IS" => Is(line_number, column_number),
		"ISNULL" => IsNull(line_number, column_number),
		"JOIN" =>Join(line_number, column_number),
		"KEY" => Key(line_number, column_number),
		"LEFT" => Left(line_number, column_number),
		"LIKE" => Like(line_number, column_number),
		"LIMIT" => Limit(line_number, column_number),
		"MATCH" => Match(line_number, column_number),
		"NATURAL" => Join(line_number, column_number),
		"NO" => No(line_number, column_number),
		"NOT" => Not(line_number, column_number),
		"NOTNULL" => NotNull(line_number, column_number),
		"NULL" => Null(line_number, column_number),
		"OF" => Of(line_number, column_number),
		"OFFSET" => Offset(line_number, column_number),
		"ON" => On(line_number, column_number),
		"OR" => Or(line_number, column_number),
		"ORDER" => Order(line_number, column_number),
		"OUTER" => Join(line_number, column_number),
		"PLAN" => Plan(line_number, column_number),
		"PRAGMA" => Pragma(line_number, column_number),
		"PRIMARY" => Primary(line_number, column_number),
		"QUERY" => Query(line_number, column_number),
		"RAISE" => Raise(line_number, column_number),
		"RECURSIVE" => Recursive(line_number, column_number),
		"REFERENCES" => References(line_number, column_number),
		"REGEXP" => Like(line_number, column_number),
		"REINDEX" => Reindex(line_number, column_number),
		"RELEASE" => Release(line_number, column_number),
		"RENAME" => Rename(line_number, column_number),
		"REPLACE" => Replace(line_number, column_number),
		"RESTRICT" => Restrict(line_number, column_number),
		"RIGHT" => Join(line_number, column_number),
		"ROLLBACK" => Rollback(line_number, column_number),
		"ROW" => Row(line_number, column_number),
		"SAVEPOINT" => Savepoint(line_number, column_number),
		"SELECT" => Select(line_number, column_number),
		"SET" => Set(line_number, column_number),
		"TABLE" => Table(line_number, column_number),
		"TEMP" => Temp(line_number, column_number),
		"TEMPORARY" => Temp(line_number, column_number),
		"THEN" => Then(line_number, column_number),
		"TO" => To(line_number, column_number),
		"TRANSACTIOIN" => Transaction(line_number, column_number),
		"TRIGGER" => Trigger(line_number, column_number),
		"UNION" => Union(line_number, column_number),
		"UPDATE" => Update(line_number, column_number),
		"USING" => Using(line_number, column_number),
		"VACUUM" => Vacuum(line_number, column_number),
		"VALUES" => Values(line_number, column_number),
		"VIEW" => View(line_number, column_number),
		"VIRTUAL" => Virtual(line_number, column_number),
		"WITH" => With(line_number, column_number),
		"WITHOUT" => Without(line_number, column_number),
		"WHEN" => When(line_number, column_number),
		"WHERE" => Where(line_number, column_number),
		_ => Ident(word, line_number, column_number)
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
				use self::Token::*;

				match character_to_token(c, self.line_number, self.column_number) {
					Some(LessThan(_,_)) | Some(GreaterThan(_,_)) | Some(Minus(_,_)) | Some(Pipe(_,_)) | Some(ForwardSlash(_,_)) => {
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
							let buffer = self.move_string_buffer();
							self.tokens.push(word_to_token(buffer, self.start_line_number, self.start_column_number));
							self.no_state(c).unwrap()
						}
					},
					None => {
						let buffer = self.move_string_buffer();
						self.tokens.push(word_to_token(buffer, self.start_line_number, self.start_column_number));
						LexerState::NoState
					}
				}
			},
			LexerState::Backtick => {
				match c {
					Some('`') => {
						let buffer = self.move_string_buffer();
						self.tokens.push(Token::Ident(buffer, self.start_line_number, self.start_column_number));
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
							self.tokens.push(Token::StringLiteral(buffer, self.start_line_number, self.start_column_number));
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
							self.tokens.push(Token::Number(buffer, self.start_line_number, self.start_column_number));
							self.no_state(c).unwrap()
						}
					}
				} else {
					let buffer = self.move_string_buffer();
					self.tokens.push(Token::Number(buffer, self.start_line_number, self.start_column_number));
					LexerState::NoState
				}
			},
			LexerState::OperatorDisambiguate { first } => {
				use self::Token::*;

				if let Some(c) = c {
					match (first, c) {
						('<', '>') => {
							self.tokens.push(NotEqual(self.start_line_number, self.start_column_number));
							LexerState::NoState
						},
						('<', '=') => {
							self.tokens.push(LessThanOrEqual(self.start_line_number, self.start_column_number));
							LexerState::NoState
						},
						('>', '=') => {
							self.tokens.push(GreaterThanOrEqual(self.start_line_number, self.start_column_number));
							LexerState::NoState
						},
						('|', '|') => {
							self.tokens.push(DoublePipe(self.start_line_number, self.start_column_number));
							LexerState::NoState
						},
						('-', '-') => {
							LexerState::LineComment
						},
						('/', '*') => {
							LexerState::BlockComment { was_prev_char_asterisk: false }
						},
						_ => {
							self.tokens.push(character_to_token(first, self.start_line_number, self.start_column_number).unwrap());
							self.no_state(c).unwrap()
						}
					}
				} else {
					self.tokens.push(character_to_token(first, self.start_line_number, self.start_column_number).unwrap());
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

	fn id(value: &str, line_number: usize, column_number: usize) -> super::Token
	{
		super::Token::Ident(value.to_string(), line_number, column_number)
	}

	fn number(value: &str, line_number: usize, column_number: usize) -> super::Token
	{
		super::Token::Number(value.to_string(), line_number, column_number)
	}

	#[test]
	fn test_sql_lexer_dontconfuseidentswithkeywords()
	{
		use super::Token::*;
		// Not: AS, Ident("df")
		assert_eq!(parse("asdf"), vec![Ident("asdf".to_string(), 1, 1)]);
		assert_eq!("asdf".to_string().tokenize(), vec![Ident("asdf".to_string(), 1, 1)]);
	}

	#[test]
	fn test_sql_lexer_escape()
	{
		use super::Token::*;
		// Escaped apostrophe
		assert_eq!(parse(r"'\''"), vec![StringLiteral("'".to_string(),1,1)]);
		assert_eq!(r"'\''".to_string().tokenize(), vec![StringLiteral("'".to_string(),1,1)]);
	}


	#[test]
	fn test_sql_lexer_numbers()
	{
		use super::Token::*;

		assert_eq!(parse("12345"), vec![number("12345",1,1)]);
		assert_eq!("12345".to_string().tokenize(), vec![number("12345",1,1)]);
		assert_eq!(parse("0.25"), vec![number("0.25",1,1)]);
		assert_eq!("0.25".to_string().tokenize(), vec![number("0.25",1,1)]);
		assert_eq!(parse("0.25 + -0.25"), vec![number("0.25",1,1), Plus(1,6), Minus(1,8), number("0.25",1,9)]);
		assert_eq!("-0.25 + 0.25".to_string().tokenize(), vec![Minus(1,1), number("0.25",1,2), Plus(1,7), number("0.25",1,9)]);
		assert_eq!("- 0.25 - -0.25".to_string().tokenize(), vec![Minus(1,1), number("0.25",1,3), Minus(1,8), Minus(1,10), number("0.25",1,11)]);
		assert_eq!("- 0.25 --0.25".to_string().tokenize(), vec![Minus(1,1), number("0.25",1,3)]);
		assert_eq!("0.25 -0.25".to_string().tokenize(), vec![number("0.25",1,1), Minus(1,6), number("0.25",1,7)]);
	}

	#[test]
	fn test_sql_lexer_query1()
	{
		use super::Token::*;

		assert_eq!(parse(" SeLECT a,    b as alias1 , c alias2, d ` alias three ` \nfRoM table1 WHERE a='Hello World'; "),
			vec![
				Select(1,2), id("a",1,9), Comma(1,10), id("b",1,15), As(1,17), id("alias1",1,20), Comma(1,27),
				id("c",1,29), id("alias2",1,31), Comma(1,37), id("d",1,39), id(" alias three ",1,41),
				From(2,1), id("table1",2,6),
				Where(2,13), id("a",2,19), Equal(2,20), StringLiteral("Hello World".to_string(),2,21), Semicolon(2,34)
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
			Select(3,3), id("d",3,10), Dot(3,11), id("id",3,12), id("departmentId",3,15), Comma(3,27), id("count",3,29), LeftParen(3,34), id("e",3,35), Dot(3,36), id("id",3,37), RightParen(3,39), id("employeeCount",3,41),
			From(4,3), id("department",4,8), id("d",4,19),
			Left(5,3), Join(5,8), id("employee",5,13), id("e",5,22), On(5,24), id("e",5,27), Dot(5,28), id("departmentId",5,29), Equal(5,42), id("d",5,44), Dot(5,45), id("id",5,46),
			Group(6,3), By(6,9), id("departmentId",6,12), Semicolon(6,24)
		]);
	}

	#[test]
	fn test_sql_lexer_operators() {
		use super::Token::*;

		assert_eq!(parse("> = >=< =><"),
			vec![
				GreaterThan(1,1), Equal(1,3), GreaterThanOrEqual(1,5), LessThan(1,7), Equal(1,9), GreaterThan(1,10), LessThan(1,11)
			]
		);

		assert_eq!(parse(" ><>> >< >"),
			vec![
				GreaterThan(1,2), NotEqual(1,3), GreaterThan(1,5), GreaterThan(1,7), LessThan(1,8), GreaterThan(1,10)
			]
		);
	}
	
	#[test]
	fn test_sql_lexer_blockcomment() {
		use super::Token::*;

		assert_eq!(parse("hello/*/a/**/,/*there, */world"), vec![
			id("hello",1,1), Comma(1,14), id("world",1,26)
		]);
		assert_eq!(parse("/ */"), vec![ForwardSlash(1,1), Asterisk(1,3), ForwardSlash(1,4)]);
		assert_eq!(parse("/**/"), vec![]);
		assert_eq!(parse("a/* test\ntest** /\nb*/c"), vec![id("a",1,1), id("c",3,4)]);
	}
}

