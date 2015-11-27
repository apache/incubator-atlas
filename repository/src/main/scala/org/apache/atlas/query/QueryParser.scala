/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.query

import org.apache.atlas.query.Expressions._

import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.{ImplicitConversions, PackratParsers}
import scala.util.parsing.input.CharArrayReader._

trait QueryKeywords {
    this: StandardTokenParsers =>

    import scala.language.implicitConversions

    protected case class Keyword(str: String)

    protected implicit def asParser(k: Keyword): Parser[String] = k.str

    protected val LIST_LPAREN = Keyword("[")
    protected val LIST_RPAREN = Keyword("]")
    protected val LPAREN = Keyword("(")
    protected val RPAREN = Keyword(")")
    protected val EQ = Keyword("=")
    protected val LT = Keyword("<")
    protected val GT = Keyword(">")
    protected val NEQ = Keyword("!=")
    protected val LTE = Keyword("<=")
    protected val GTE = Keyword(">=")
    protected val COMMA = Keyword(",")
    protected val AND = Keyword("and")
    protected val OR = Keyword("or")
    protected val PLUS = Keyword("+")
    protected val MINUS = Keyword("-")
    protected val STAR = Keyword("*")
    protected val DIV = Keyword("/")
    protected val DOT = Keyword(".")

    protected val SELECT = Keyword("select")
    protected val FROM = Keyword("from")
    protected val WHERE = Keyword("where")
    protected val GROUPBY = Keyword("groupby")
    protected val LOOP = Keyword("loop")
    protected val ISA = Keyword("isa")
    protected val IS = Keyword("is")
    protected val HAS = Keyword("has")
    protected val AS = Keyword("as")
    protected val TIMES = Keyword("times")
    protected val WITHPATH = Keyword("withPath")
}

trait ExpressionUtils {

    def loop(input: Expression, l: (Expression, Option[Literal[Integer]], Option[String])) = l match {
        case (c, None, None) => input.loop(c)
        case (c, t, None) => input.loop(c, t.get)
        case (c, None, Some(a)) => input.loop(c).as(a)
        case (c, t, Some(a)) => input.loop(c, t.get).as(a)
    }

    def select(input: Expression, s: List[(Expression, Option[String])]) = {
        val selList = s.map { t =>
            t._2 match {
                case None => t._1.as(s"${t._1}")
                case _ => t._1.as(t._2.get)
            }
        }
        input.select(selList: _*)
    }

    def leftmostId(e: Expression) = {
        var le: IdExpression = null
        e.traverseUp { case i: IdExpression if le == null => le = i}
        le
    }

    def notIdExpression = new PartialFunction[Expression, Expression] {
        def isDefinedAt(x: Expression): Boolean = !x.isInstanceOf[IdExpression]

        def apply(e: Expression) = e
    }

    def replaceIdWithField(id: IdExpression, fe: UnresolvedFieldExpression): PartialFunction[Expression, Expression] = {
        case e: IdExpression if e == id => fe
    }

    def merge(snglQuery1: Expression, sngQuery2: Expression): Expression = {
        val leftSrcId = leftmostId(sngQuery2)
        sngQuery2.transformUp(replaceIdWithField(leftSrcId, snglQuery1.field(leftSrcId.name)))
    }
}

class QueryParser extends StandardTokenParsers with QueryKeywords with ExpressionUtils with PackratParsers {

    import scala.language.higherKinds

    private val reservedWordsDelims: Seq[String] = this.
        getClass.getMethods.filter(_.getReturnType == classOf[Keyword]).map(_.invoke(this).asInstanceOf[Keyword].str)

    private val (queryreservedWords: Seq[String], querydelims: Seq[String]) =
        reservedWordsDelims.partition(s => s.charAt(0).isLetter)

    override val lexical = new QueryLexer(queryreservedWords, querydelims)

    def apply(input: String): Either[NoSuccess, Expression] = {
        phrase(queryWithPath)(new lexical.Scanner(input)) match {
            case Success(r, x) => Right(r)
            case f@Failure(m, x) => Left(f)
            case e@Error(m, x) => Left(e)
        }
    }

    def queryWithPath = query ~ opt(WITHPATH) ^^ {
      case q ~ None => q
      case q ~ p => q.path()
    }

    def query: Parser[Expression] = rep1sep(singleQuery, opt(COMMA)) ^^ { l => l match {
        case h :: Nil => h
        case h :: t => t.foldLeft(h)(merge(_, _))
    }
    }

    def singleQuery = singleQrySrc ~ opt(loopExpression) ~ opt(selectClause) ^^ {
        case s ~ None ~ None => s
        case s ~ l ~ None => loop(s, l.get)
        case s ~ l ~ sel if l.isDefined => select(loop(s, l.get), sel.get)
        case s ~ None ~ sel => select(s, sel.get)
    }

    /**
     * A SingleQuerySrc can have the following forms:
     * 1. FROM id [WHERE] [expr] -> from optionally followed by a filter
     * 2. WHERE expr -> where clause, FROM is assumed to be the leftmost Id in the where clause
     * 3. expr (that is not an IdExpression)  -> where clause, FROM is assumed to be the leftmost Id in the expr
     * 4. Id [WHERE] [expr] -> from optionally followed by a filter
     *
     * @return
     */
    def singleQrySrc: Parser[Expression] = FROM ~ fromSrc ~ opt(WHERE) ~ opt(expr ^? notIdExpression) ^^ {
        case f ~ i ~ w ~ None => i
        case f ~ i ~ w ~ c => i.where(c.get)
    } |
        WHERE ~ (expr ^? notIdExpression) ^^ { case w ~ e => {
            val lId = leftmostId(e)
            if (lId == null) {
                failure("cannot infer Input from the where clause")
            }
            lId.where(e)
        }
        } |
        expr ^? notIdExpression ^^ { case e => {
            val lId = leftmostId(e)
            if (lId == null) {
                failure("cannot infer Input from the where clause")
            }
            lId.where(e)
        }
        } |
        fromSrc ~ opt(WHERE) ~ opt(expr ^? notIdExpression) ^^ {
            case i ~ w ~ None => i
            case i ~ w ~ c => i.where(c.get)
        }

    def fromSrc = identifier ~ AS ~ alias ^^ { case s ~ a ~ al => s.as(al)} |
        identifier


    def loopExpression: Parser[(Expression, Option[Literal[Integer]], Option[String])] =
        LOOP ~ (LPAREN ~> query <~ RPAREN) ~ opt(intConstant <~ TIMES) ~ opt(AS ~> alias) ^^ {
            case l ~ e ~ None ~ a => (e, None, a)
            case l ~ e ~ Some(i) ~ a => (e, Some(int(i)), a)
        }

    def selectClause: Parser[List[(Expression, Option[String])]] = SELECT ~ rep1sep(selectExpression, COMMA) ^^ {
        case s ~ cs => cs
    }

    def selectExpression: Parser[(Expression, Option[String])] = expr ~ opt(AS ~> alias) ^^ {
        case e ~ a => (e, a)
    }

    def expr: Parser[Expression] = compE ~ opt(rep(exprRight)) ^^ {
        case l ~ None => l
        case l ~ Some(r) => r.foldLeft(l) { (l, r) => l.logicalOp(r._1)(r._2)}
    }

    def exprRight = (AND | OR) ~ compE ^^ { case op ~ c => (op, c)}

    def compE =
        arithE ~ (LT | LTE | EQ | NEQ | GT | GTE) ~ arithE ^^ { case l ~ op ~ r => l.compareOp(op)(r)} |
            arithE ~ (ISA | IS) ~ ident ^^ { case l ~ i ~ t => l.isTrait(t)} |
            arithE ~ HAS ~ ident ^^ { case l ~ i ~ f => l.hasField(f)} |
            arithE

    def arithE = multiE ~ opt(rep(arithERight)) ^^ {
        case l ~ None => l
        case l ~ Some(r) => r.foldLeft(l) { (l, r) => l.arith(r._1)(r._2)}
    }

    def arithERight = (PLUS | MINUS) ~ multiE ^^ { case op ~ r => (op, r)}

    def multiE = atomE ~ opt(rep(multiERight)) ^^ {
        case l ~ None => l
        case l ~ Some(r) => r.foldLeft(l) { (l, r) => l.arith(r._1)(r._2)}
    }

    def multiERight = (STAR | DIV) ~ atomE ^^ { case op ~ r => (op, r)}

    def atomE = literal | identifier | LPAREN ~> expr <~ RPAREN | listLiteral

    def listLiteral = LIST_LPAREN ~ rep1sep(literal, COMMA) ~ LIST_RPAREN ^^ {
        case lp ~ le ~ rp => list(le)
    }

    def identifier = rep1sep(ident, DOT) ^^ { l => l match {
        case h :: Nil => id(h)
        case h :: t => {
            t.foldLeft(id(h).asInstanceOf[Expression])(_.field(_))
        }
    }
    }

    def alias = ident | stringLit

    def literal = booleanConstant ^^ {
        boolean(_)
        } |
        intConstant ^^ {
            int(_)
        } |
        longConstant ^^ {
            long(_)
        } |
        floatConstant ^^ {
            float(_)
        } |
        doubleConstant ^^ {
            double(_)
        } |
        stringLit ^^ {
            string(_)
        }

    def booleanConstant: Parser[String] =
        elem("int", _.isInstanceOf[lexical.BooleanLiteral]) ^^ (_.chars)

    def intConstant: Parser[String] =
        elem("int", _.isInstanceOf[lexical.IntLiteral]) ^^ (_.chars)

    def longConstant: Parser[String] =
        elem("int", _.isInstanceOf[lexical.LongLiteral]) ^^ (_.chars)

    def floatConstant: Parser[String] =
        elem("int", _.isInstanceOf[lexical.FloatLiteral]) ^^ (_.chars)

    def doubleConstant: Parser[String] =
        elem("int", _.isInstanceOf[lexical.DoubleLiteral]) ^^ (_.chars)

}

class QueryLexer(val keywords: Seq[String], val delims: Seq[String]) extends StdLexical with ImplicitConversions {

    case class BooleanLiteral(chars: String) extends Token {
        override def toString = chars
    }

    case class IntLiteral(chars: String) extends Token {
        override def toString = chars
    }

    case class LongLiteral(chars: String) extends Token {
        override def toString = chars
    }

    case class FloatLiteral(chars: String) extends Token {
        override def toString = chars
    }

    case class DoubleLiteral(chars: String) extends Token {
        override def toString = chars
    }

    reserved ++= keywords.flatMap(w => allCaseVersions(w))

    delimiters ++= delims

    override lazy val token: Parser[Token] =
        (
            (trueP | falseP)
                | longConstant ^^ LongLiteral
                | intConstant ^^ IntLiteral
                | floatConstant ^^ FloatLiteral
                | dubConstant ^^ DoubleLiteral
                | identifier ^^ processIdent
                | string ^^ StringLit
                | EofCh ^^^ EOF
                | '\'' ~> failure("unclosed string literal")
                | '"' ~> failure("unclosed string literal")
                | delim
                | '.' ^^^ new Keyword(".")
                | failure("illegal character")
            )

    override def identChar = letter | elem('_')

    def identifier = identChar ~ (identChar | digit).* ^^ { case first ~ rest => (first :: rest).mkString} |
        '`' ~> chrExcept('`', '\n', EofCh).* <~ '`' ^^ {
            _ mkString ""
        }

    override def whitespace: Parser[Any] =
        (whitespaceChar
            | '/' ~ '*' ~ comment
            | '/' ~ '/' ~ chrExcept(EofCh, '\n').*
            | '#' ~ chrExcept(EofCh, '\n').*
            | '/' ~ '*' ~ failure("unclosed comment")
            ).*

    protected override def comment: Parser[Any] = (
        commentChar.* ~ '*' ~ '/'
        )

    protected def commentChar = chrExcept(EofCh, '*') | '*' ~ not('/')

    def string = '\"' ~> chrExcept('\"', '\n', EofCh).* <~ '\"' ^^ {
        _ mkString ""
    } |
        '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^ {
            _ mkString ""
        }

    def zero: Parser[String] = '0' ^^^ "0"

    def nonzero = elem("nonzero digit", d => d.isDigit && d != '0')

    def sign = elem("sign character", d => d == '-' || d == '+')

    def exponent = elem("exponent character", d => d == 'e' || d == 'E')


    def intConstant = opt(sign) ~> zero | intList

    def intList = opt(sign) ~ nonzero ~ rep(digit) ^^ { case s ~ x ~ y => (optString("", s) :: x :: y) mkString ""}

    def fracPart: Parser[String] = '.' ~> rep(digit) ^^ { r =>
        "." + (r mkString "")
    }

    def expPart = exponent ~ opt(sign) ~ rep1(digit) ^^ { case e ~ s ~ d =>
        e.toString + optString("", s) + d.mkString("")
    }

    def dubConstant = opt(sign) ~ digit.+ ~ fracPart ~ opt(expPart) ^^ {
        case s ~ i ~ f ~ e => {
            optString("", s) + (i mkString "") + f + optString("", e)
        }
    }

    def floatConstant = opt(sign) ~ digit.* ~ fracPart ~ 'f' ^^ { case s ~ i ~ fr ~ f =>
        optString("", s) + i + fr
    } | opt(sign) ~ digit.+ ~ opt(fracPart) ~ 'f' ^^ { case s ~ i ~ fr ~ f =>
        optString("", s) + i + optString("", fr)
    }

    def longConstant = intConstant ~ 'l' ^^ { case i ~ l => i}

    def trueP = 't' ~ 'r' ~ 'u' ~ 'e' ^^^ BooleanLiteral("true")

    def falseP = 'f' ~ 'a' ~ 'l' ~ 's' ~ 'e' ^^^ BooleanLiteral("false")

    private def optString[A](pre: String, a: Option[A]) = a match {
        case Some(x) => pre + x.toString
        case None => ""
    }

    /** Generate all variations of upper and lower case of a given string */
    def allCaseVersions(s: String, prefix: String = ""): Stream[String] = {
        if (s.isEmpty) {
            Stream(prefix)
        } else {
            allCaseVersions(s.tail, prefix + s.head.toLower) #:::
                allCaseVersions(s.tail, prefix + s.head.toUpper)
        }
    }
}