use ast;
use lexer;

use lexer::Token;
use lexer::Tokenizer;
use ast::*;

pub struct ParseError
{
	message: &'static str,
	location: Option<Token>,
	expecting: Option<Token>,
}

pub enum ParseResult
{
	Error(ParseError),
	Success(Ast),
}

trait Parse
{
	fn parse_tokens(tokens: Vec<Token>) -> ParseResult;
}

fn parse<T: Parse>(s: &str) -> T
{
	parse_tokens(s.tokenize())
}
