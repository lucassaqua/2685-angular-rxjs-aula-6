import { switchMap, map, tap, filter, debounceTime, throwError, catchError, EMPTY, of, distinctUntilChanged } from 'rxjs';
import { Component } from '@angular/core';
import { LivroService } from 'src/app/service/livro.service';
import { Item, LivrosResultado } from 'src/app/models/interfaces';
import { LivroVolumeInfo } from 'src/app/models/livroVolumeInfo';
import { FormControl } from '@angular/forms';

const PAUSA = 300;

@Component({
  selector: 'app-lista-livros',
  templateUrl: './lista-livros.component.html',
  styleUrls: ['./lista-livros.component.css']
})
export class ListaLivrosComponent {

  campoBusca = new FormControl(); // o FormControl() retorna um observable
  mensagemErro = ''
  livrosResultado: LivrosResultado;

  constructor(private service: LivroService) {
    console.log("testObservable");
    this.testObservable.pipe(
      tap((result) => {
        console.log('result  tap 1 ');
        console.log(result);
      }),
      tap((result) => {
        console.log('result  tap 2 ');
        result.push(" -t2");
        console.log(result);
        // return result.map((item) => item + " - t1");
        //return result;

      }),
      map((resultado) => resultado.map((item) => item + " - map")),
      tap((retornoAPI) =>{
        console.log("result  tap 3")
        console.log(retornoAPI)
      } )
    ).subscribe((resultado) => {
      console.log('result  subscribe');
      console.log(resultado.concat(" - subscribe"));
    }
    )
  }

  testObservable = of(["Lucas Cardoso", "Julio Cesar", "Lucas Lima", "Lucas Silva", "Julio Cesar", "Lucas Lima", "Lucas Silva"])

  // totalDeLivros$ = this.campoBusca.valueChanges
  // .pipe(
  //   debounceTime(PAUSA),
  //   filter((valorDigitado) => valorDigitado.length >= 3),
  //   tap(() => console.log('Fluxo inicial')),
  //   switchMap((valorDigitado) => this.service.buscar(valorDigitado)),
  //   map(resultado => this.livrosResultado = resultado),
  //   catchError(erro => {
  //     console.log(erro)
  //     return of()
  //   })
  // )

   // o valueChanges é 'chamado' sempre que o valor do campo de busca muda
  livrosEncontrados$ = this.campoBusca.valueChanges // o $ do final da variável indica que ela representa um observable
    .pipe(
      debounceTime(PAUSA), // espera 300 ms
      filter((valorDigitado) => valorDigitado.length >= 3), // quando eu uso o filter aqui, o fluxo só continua se a condição for atendida. Senão os operadoresa sseguintes não são chamados.
            tap((result) => {
        console.log('Fluxo inicial')
        console.log(result)
      }),
      // distinctUntilChanged(), // o distinctUntilChanged garante que o valor digitado só seja emitido se for diferente do valor anterior. Isso evita que o debounceTime seja chamado várias vezes com o mesmo valor.
      switchMap((valorDigitado) => this.service.buscar(valorDigitado)), // switchMap garante que se o usuário disparar várias buscas em sequência, apenas a última ativa seja processada até o fim. Isso aconrece, caso o valor digitado mude antes do tempo de espera do debounceTime ou do tempo que a requisição leva para ser concluida. Observe que, antes do switchmap, o valor observado era o digitado no campo busca, o que pode ser visualizado no log do tap acima, e depois do swicthmap o valor observado é o retorno do service.buscar() o que pode ser visto no log do tap abaixo, é esse valor ( que é um Observable ) que é passado adiante.  É Interessante notar que nesssa busca trabalhamos com dois Observables, o campoBusca e o retorno do service.buscar(), e para o retorno do service.buscar() trazer novos valores ele dependde de um novo valor do campoBusca, por isso observo primeiro o campoBusca e depois o retorno do service.buscar() dentro do switchMap.  Se eu parasse o pipe aqui nessa linha, o abservable que iria para a variavel livrosEncontrados$ servia o retono do service.buscar().
      map(resultado => this.livrosResultado = resultado),
      tap((retornoAPI) => {
        console.log("retornoAPI")
        console.log(retornoAPI)
      }),
      map(resultado => {
        console.log("resultado.items ----")
        console.log(resultado.items)
        return resultado.items ?? [] // O trecho "?? []" significa que se resultado.items for null ou undefined, o valor retornado será um array vazio. Isso irá evitar que aconteça um erro no livrosResultadoParaLivros() no items.map ( o erro aocnte pq n dá pra dar .map num undefined), quando nenhum resultado for encontrado na busca. Pois quando a busca não retornar nenhum resultado, o resultado.items na linha acima será undefined

      } ), // resultado será resultado.items, se resultado.items for diferente de null ou undefined. Senhão será []
      map((items) => this.livrosResultadoParaLivros(items)),
      catchError((erro) => { // o catchError é chamado quando ocorre um erro na requisição. O erro pode ser tratado aqui ou passado adiante. Se eu não tratar o erro aqui, ele será passado adiante e o subscribe não será chamado. Mas o catchError não interrompe o fluxo, nem retorna um observable, para retornar um observable é necessário usar o método throwError().  É importante notar que quando um erro aconrece o codigo no subscribe não é executado.
        // this.mensagemErro ='Ops, ocorreu um erro. Recarregue a aplicação!'
        // return EMPTY    // o EMPTY é um observable que não emite nenhum valor e completa imediatamente o ciclo de vida do observable. Isso é útil quando você quer interromper o fluxo sem emitir nenhum valor.  É importante notar que se o ciclo de vida do obervable for interrompido, mesmo que hajam mudanças no campo de de busca, o observable não será chamado novamente, sendo necessário reiniciar o observable em um médoto ou recarregando a página.
        console.log(erro)
        return throwError(() => new Error(this.mensagemErro ='Ops, ocorreu um erro. Recarregue a aplicação!')) // pra testar o funcionamento desse erro, eu posso colocar uma url errada e executar o projeto.   O throwError retorna um novo observable que emite imediatamente o erro e termina seu ciclo de vida.
      })
    )

  livrosResultadoParaLivros(items: Item[]): LivroVolumeInfo[] {
    this.soma(1, 2);
    this.digaOi();
    this.quadrado(2);
    return items.map(item => {
      return new LivroVolumeInfo(item)
    })
  }

  // Exemplos de arrow functions
  soma = (a: number, b: number) => a + b; // somar uma uma arrow function, como as outras aqui, mas é uma arrow function. Ela retorna implicitamente a + b. Se a função tiver apenas uma expressão simples, como essa, não precisa usar {}, nem o return, mas caso contrário, precisará de ambos.
  digaOi = () => console.log("Oi");
  quadrado = n => n * n; // quando a função tem apenas um parâmetro não precisar usar () em volta do parâmetro. Só precisar usar () se quiser tipar o parâmetro.

}


