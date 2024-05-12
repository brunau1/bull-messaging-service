// os nomes das filas devem incluir um dos prefixos abaixo para facilitar a identificação do metodo processador de cada fila
export enum QueuePrefixes {
  WITHDRAW = 'withdraw_context',
  DEPOSIT = 'deposit_context',
  WITHDRAW_REJECTION = 'withdraw_error_context',
  CASHIN_WEBHOOK = 'cashin_webhook_context',
  CASHOUT_WEBHOOK = 'cashout_webhook_context',
}
